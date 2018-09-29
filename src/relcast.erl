-module(relcast).

%% API exports
-export([]).

%%====================================================================
%% Callback functions
%%====================================================================
-callback init(Arguments :: any()) -> {ok, State :: term()}.
-callback restore(OldState :: term(), NewState :: term()) -> {ok, State :: term()}.
-callback serialize(State :: term()) -> Binary :: binary().
-callback deserialize(Binary :: binary()) -> State :: term().
-callback handle_message(Message :: message(), ActorId :: pos_integer(), State :: term()) ->
    {NewState :: term(), Actions :: actions() | defer}.
-callback handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                     State :: term()) ->
   {reply, Reply :: term(), NewState :: term()} |
   {reply, Reply :: term(), NewState :: term(), timeout()} |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), timeout()} |
   {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
   {stop, Reason :: term(), NewState :: term()}.
-callback handle_cast(Request :: term(), State :: term()) ->
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), timeout()} |
{stop, Reason :: term(), NewState :: term()}.

-type actions() :: [ {send, Message :: message()} |
                     {stop, Timeout :: timeout()} | new_epoch ].

-type message() ::
        {unicast, Index::pos_integer(), Msg::message_value()} |
        {multicast, Msg::message_value()}.

-type message_key_prefix() :: <<_:128>>.
-type message_value() ::
        {message_key_prefix(), binary()} |
        binary().

%%====================================================================
%% Callback functions
%%====================================================================

-record(state, {
          db,
          module,
          modulestate,
          id,
          ids,
          last_sent = #{},
          pending_acks = #{},
          key_count = 0,
          bitfieldsize
         }).

-export([start/4, deliver/3, take/2, ack/3]).

start(ActorID, ActorIDs, Module, Arguments) ->
    DataDir = proplists:get_value(data_dir, Arguments),
    DBOptions = db_options(length(ActorIDs)),
    {ok, DB} = rocksdb:open(DataDir, DBOptions),
    case erlang:apply(Module, init, Arguments) of
        {ok, ModuleState0} ->
            ModuleState = case rocksdb:get(DB, <<"module_state">>, []) of
                              {ok, SerializedModuleState} ->
                                  {ok, OldModuleState} = Module:deserialize(SerializedModuleState),
                                  {ok, RestoredModuleState} = Module:restore(OldModuleState, ModuleState0),
                                  RestoredModuleState;
                              not_found ->
                                  ModuleState0
                          end,
            LastKey = get_last_key(DB),
            %% TODO try to deliver any old queued inbound messages
            BitFieldSize = round_to_nearest_byte(length(ActorIDs) + 1), %% one bit for unicast/multicast
            {ok, #state{module=Module, id=ActorID, ids=ActorIDs, modulestate=ModuleState, db=DB, key_count=LastKey, bitfieldsize=BitFieldSize}};
        _ ->
            error
    end.

deliver(Message, FromActorID, State = #state{module=Module, key_count=KeyCount, modulestate=ModuleState, db=DB, bitfieldsize=BitfieldSize}) ->
    Key = make_inbound_key(KeyCount), %% some kind of predictable, monotonic key
    ok = rocksdb:put(DB, Key, <<1:1/integer, FromActorID:BitfieldSize/integer, Message/binary>>, [{sync, true}]),
    case Module:handle_message(Message, FromActorID, ModuleState) of
        defer ->
            %% leave the message queued but we can ACK it
            {ok, State};
        {NewModuleState, Actions} ->
            {ok, Batch} = rocksdb:batch(),
            %% write new output messages, update the state and delete the message atomically
            ok = rocksdb:batch_put(Batch, <<"module_state">>, Module:serialize(NewModuleState)),
            ok = rocksdb:batch_delete(Batch, Key),
            case handle_actions(Actions, Batch, State) of
                {ok, NewState} ->
                    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
                    {ok, NewState#state{modulestate=NewModuleState}};
                {stop, Timeout, NewState} ->
                    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
                    {stop, Timeout, NewState#state{modulestate=NewModuleState}}
            end
    end.

take(ForActorID, State = #state{bitfieldsize=BitfieldSize, db=DB}) ->
    %% we need to find the first "unacked" message for this actor
    %% we should remember the last acked message for this actor ID and start there
    %% check if there's a pending ACK and use that to find the "last" key, if present
    case maps:get(ForActorID, State#state.pending_acks, undefined) of
        {Ref, Key} ->
            case rocksdb:get(DB, Key, []) of
                {ok, <<1:1/integer, ForActorID:15/integer, Value/binary>>} ->
                    {ok, Ref, Value, State};
                {ok, <<0:1/integer, _:(BitfieldSize)/integer, Value/binary>>} ->
                    {ok, Ref, Value, State};
                not_found ->
                    %% something strange is happening, try again
                    take(ForActorID, State#state{pending_acks=maps:delete(ForActorID, State#state.pending_acks)})
            end;
        _ ->
            StartKey = maps:get(ForActorID, State#state.last_sent, min_outbound_key()), %% default to the "first" key"
            %% iterate until we find a key for this actor
            case find_next_outbound(ForActorID, StartKey, DB, State#state.bitfieldsize) of
                {Key, Msg} ->
                    Ref = make_ref(),
                    {ok, Ref, Msg, State#state{pending_acks=maps:put(ForActorID, {Ref, Key}, State#state.pending_acks)}};
                not_found ->
                    not_found
            end
    end.

ack(FromActorID, Ref, State = #state{bitfieldsize=BitfieldSize, db=DB}) ->
    case maps:get(FromActorID, State#state.pending_acks, undefined) of
        {Ref, Key} ->
            case rocksdb:get(DB, Key, []) of
                {ok, <<1:1/integer, FromActorID:15/integer, _Value/binary>>} ->
                    %% unicast message, fine to delete now
                    ok = rocksdb:delete(DB, Key, [{sync, true}]);
                {ok, <<0:1/integer, SentTo:(BitfieldSize)/integer, _Value/binary>>} ->
                    %% multicast message, see if all the bits have gone 0
                    case SentTo bxor (1 bsl (State#state.bitfieldsize - FromActorID)) of
                        0 ->
                            %% time to delete
                            ok = rocksdb:delete(DB, Key, [{sync, true}]);
                        _Remaining ->
                            %% flip the bit for this actor
                            ActorIDStr = io_lib:format("+~b", [FromActorID+1]),
                            ok = rocksdb:merge(DB, <<"bitmap">>, list_to_binary(ActorIDStr), [{sync, true}])
                    end;
                not_found ->
                    %% something strange is happening
                    ok
            end,
            NewPending = maps:delete(FromActorID, State#state.pending_acks),
            {ok, State#state{pending_acks=NewPending, last_sent=maps:put(FromActorID, Key, State#state.last_sent)}};
        undefined ->
            {ok, State}
    end.


%%====================================================================
%% Internal functions
%%====================================================================

%% write all resulting messages and keys in an atomic batch
handle_actions([], _Batch, State) ->
    {ok, State};
handle_actions([{multicast, Message}|Tail], Batch, State = #state{key_count=KeyCount, bitfieldsize=BitfieldSize}) ->
    Bitfield = make_bitfield(BitfieldSize),
    rocksdb:batch_put(Batch, make_outbound_key(KeyCount), <<0:1/integer, Bitfield:BitfieldSize/bits, Message/binary>>),
    handle_actions(Tail, Batch, State#state{key_count=KeyCount+1});
handle_actions([{unicast, ToActorID, Message}|Tail], Batch, State = #state{key_count=KeyCount}) ->
    rocksdb:batch_put(Batch, make_outbound_key(KeyCount), <<1:1/integer, ToActorID:15/integer, Message/binary>>),
    handle_actions(Tail, Batch, State#state{key_count=KeyCount+1});
handle_actions([{stop, Timeout}|_Tail], _Batch, State) ->
    {stop, Timeout, State}.

make_bitfield(BitfieldSize) ->
    << <<1:1/integer>> || _ <- lists:seq(1, BitfieldSize) >>.

db_options(NumActors) ->
    [
     {create_if_missing, true},
     {max_open_files, 1024},
     {max_log_file_size, 100*1024*1024},
     {merge_operator, {bitset_merge_operator, round_to_nearest_byte(NumActors+1)}}
    ].

round_to_nearest_byte(Bits) ->
    case Bits rem 8 of
        0 ->
            Bits;
        Extra ->
            Bits + (8 - Extra)
    end.

%% get the maximum key ID used
get_last_key(DB) ->
    InIter = rocksdb:iterator(DB, [{iterate_upper_bound, max_inbound_key()}]),
    {ok, <<"i", Num:10/binary>>, _} = rocksdb:iterator_move(InIter, last),
    MaxInbound = list_to_integer(binary_to_list(InIter)),
    rocksdb:iterator_close(InIter),
    OutIter = rocksdb:iterator(DB, [{iterate_upper_bound, max_outbound_key()}]),
    {ok, <<"o", Num:10/binary>>, _} = rocksdb:iterator_move(OutIter, last),
    MaxInbound = list_to_integer(binary_to_list(InIter)),
    rocksdb:iterator_close(OutIter),
    max(InIter, OutIter).

%% iterate the outbound messages until we find one for this ActorID
find_next_outbound(ActorID, StartKey, DB, ActorCount) ->
    Iter = rocksdb:iterator(DB, [{iterate_upper_bound, max_outbound_key()}]),
    Res = rocksdb:iterator_move(Iter, StartKey),
    find_next_outbound_(ActorID, Res, Iter, ActorCount).

find_next_outbound_(_ActorId, {error, _}, Iter, _ActorCount) ->
    rocksdb:iterator_close(Iter),
    not_found;
find_next_outbound_(ActorID, {ok, Key, <<1:1/integer, ActorID:15/integer, Value/binary>>}, Iter, _ActorCount) ->
    %% unicast message for this actor
    rocksdb:iterator_close(Iter),
    {Key, Value};
find_next_outbound_(ActorID, {ok, Key, <<1:1/integer, Tail/binary>>}, Iter, ActorCount) ->
    <<ActorMask:ActorCount/integer-unsigned-big, Value/binary>> = Tail,
    case ActorMask band (1 bsl (ActorCount - ActorID)) of
        0 ->
            %% not for us, keep looking
            find_next_outbound_(ActorID, rocksdb:iterator_move(Iter, next), Iter, ActorCount);
        _ ->
            %% multicast message with the high bit set for this actor
            rocksdb:iterator_close(Iter),
            {Key, Value}
    end.

min_inbound_key() ->
    <<"i0000000000">>.

max_inbound_key() ->
    <<"i4294967296">>.

min_outbound_key() ->
    <<"o0000000000">>.

max_outbound_key() ->
    <<"o4294967296">>.

make_inbound_key(KeyCount) ->
    list_to_binary(io_lib:format("i~10..0b", [KeyCount])).

make_outbound_key(KeyCount) ->
    list_to_binary(io_lib:format("o~10..0b", [KeyCount])).
