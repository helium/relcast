-module(relcast).

%% Relcast's job is ensure a consistent state for consensus protocols. It
%% provides atomic updates to the consensus state, the inbound message queue and
%% the outbound message queue. It does this by serializing all inbound messages
%% to disk before attempting to process them, by serializing the new module
%% state and any outbound messages to disk and deleting the inbound message
%% after processing the message. Assuming no disk failures, the Erlang process,
%% the Erlang VM or the host operating system should be able to fail at any time
%% and recover where it left off.
%%
%% Relcast does this using 3 kinds of keys
%%
%% * <<"stored_module_state">> - this key stores the latest serialized state of the callback
%%                        module's state. It is only read back from disk on recovery.
%%                        This key is overwritten every time the module handles
%%                        a message or an event.
%% * <<"oXXXXXXXXXX">>  - an outbound key, representing a message this instance
%%                        wishes to send to another peer.
%% * <<"iXXXXXXXXXX">>  - an inbound key, this represents a message arriving
%%                        that has not been handled yet.
%%
%%  The 10 Xs in the inbound and outbound keys represent a strictly monotonic
%%  counter that can hold 2^32 messages. They are prefixed with their direction
%%  so we can efficiently iterate over them independently. The 32 bit integer is
%%  printed in left zero padded decimal so that the keys sort lexiographically.
%%
%%  Inbound values are stored in the form <<ActorID:16/integer, Value/binary>>.
%%
%%  Outbound values come in 2 types; unicast and multicast.
%%
%%  Unicast values look like this: <<1:1/bits, ActorID:15/integer, Value/binary>>
%%  and are only intended for delivery to a single peer, identified by ActorID.
%%  Once the designated Actor has ACKed the message, the key can be deleted.
%%
%%  Multicast values look like this:
%%  <<0:1/bits, ActorBitMask:BitmaskSize/integer, Value/binary>> and are
%%  intended to be delivered to every other actor in the consensus group. Each
%%  time a send to one of the peers is ACKed, the bit for that actor is set to
%%  0. Once all the bits have been set to 0, the key can be deleted.

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
-callback handle_command(Request :: term(), State :: term()) ->
   {reply, Reply :: term(), Actions :: actions(), NewState :: term()}.
-callback terminate(Reason :: term(), NewState :: term()) -> any().
-optional_callbacks([terminate/2]).

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
          db :: rocksdb:db_handle(),
          module :: atom(),
          modulestate :: any(),
          id :: pos_integer(),
          ids :: [pos_integer()],
          last_sent = #{} :: #{pos_integer() => binary()},
          pending_acks = #{} :: #{pos_integer() => {reference(), binary()}},
          key_count = 0 :: non_neg_integer(),
          bitfieldsize :: pos_integer()
         }).

-export([start/4, start/5, command/2, deliver/3, take/2, ack/3, stop/2]).

start(ActorID, ActorIDs, Module, Arguments) ->
    start(ActorID, ActorIDs, Module, Arguments, []).

start(ActorID, ActorIDs, Module, Arguments, RelcastOptions) ->
    DataDir = proplists:get_value(data_dir, RelcastOptions),
    DBOptions = db_options(length(ActorIDs)),
    {ok, DB} = rocksdb:open(DataDir, DBOptions),
    case erlang:apply(Module, init, Arguments) of
        {ok, ModuleState0} ->
            ModuleState = case rocksdb:get(DB, <<"stored_module_state">>, []) of
                              {ok, SerializedModuleState} ->
                                  OldModuleState = Module:deserialize(SerializedModuleState),
                                  {ok, RestoredModuleState} = Module:restore(OldModuleState, ModuleState0),
                                  RestoredModuleState;
                              not_found ->
                                  ModuleState0
                          end,
            LastKey = get_last_key(DB),
            BitFieldSize = round_to_nearest_byte(length(ActorIDs)) - 1, %% one bit for unicast/multicast
            %% try to deliver any old queued inbound messages
            handle_pending_inbound(#state{module=Module, id=ActorID,
                                               ids=ActorIDs,
                                               modulestate=ModuleState, db=DB,
                                               key_count=LastKey+1,
                                               bitfieldsize=BitFieldSize});
        _ ->
            error
    end.

command(Message, State = #state{module=Module, modulestate=ModuleState, db=DB}) ->
    {reply, Reply, Actions, NewModuleState} = Module:handle_command(Message, ModuleState),
    {ok, Batch} = rocksdb:batch(),
    %% write new output messages & update the state atomically
    case handle_actions(Actions, Batch, State#state{modulestate=NewModuleState}) of
        {ok, NewState} ->
            ok = rocksdb:batch_put(Batch, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
            case handle_pending_inbound(NewState#state{modulestate=NewModuleState}) of
                {ok, NewerState} ->
                    {Reply, NewerState};
                {stop, Timeout, NewerState} ->
                    {stop, Reply, Timeout, NewerState}
            end;
        {stop, Timeout, NewState} ->
            ok = rocksdb:batch_put(Batch, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
            {stop, Reply, Timeout, NewState#state{modulestate=NewModuleState}}
    end.

deliver(Message, FromActorID, State0 = #state{key_count=KeyCount, db=DB}) ->
    Key = make_inbound_key(KeyCount), %% some kind of predictable, monotonic key
    ok = rocksdb:put(DB, Key, <<FromActorID:16/integer, Message/binary>>, [{sync, true}]),
    State = State0#state{key_count=KeyCount+1},
    case handle_pending_inbound(State) of
        {ok, NewState} ->
            case rocksdb:get(DB, Key, []) of
                {ok, <<FromActorID:16/integer, Message/binary>>} ->
                    %% key is still there, we deferred it
                    {defer, NewState};
                _ ->
                    {ok, NewState}
            end;
        {stop, Timeout, NewState} ->
            {stop, Timeout, NewState}
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
                    take(ForActorID, State#state{pending_acks=maps:remove(ForActorID, State#state.pending_acks)})
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
                    Padding = BitfieldSize - length(State#state.ids),
                    Bit = length(State#state.ids) - FromActorID,
                    %% multicast message, see if all the bits have gone 0
                    case (SentTo bsr Padding) bxor (1 bsl Bit) of
                        0 ->
                            %% time to delete
                            ok = rocksdb:delete(DB, Key, [{sync, true}]);
                        _Remaining ->
                            %% flip the bit for this actor
                            ActorIDStr = io_lib:format("-~b", [FromActorID]),
                            ok = rocksdb:merge(DB, Key, list_to_binary(ActorIDStr), [{sync, true}])
                    end;
                not_found ->
                    %% something strange is happening
                    ok
            end,
            NewPending = maps:remove(FromActorID, State#state.pending_acks),
            {ok, State#state{pending_acks=NewPending, last_sent=maps:put(FromActorID, Key, State#state.last_sent)}};
        _ ->
            {ok, State}
    end.

stop(Reason, State = #state{module=Module, modulestate=ModuleState})->
    case erlang:function_exported(Module, terminate, 2) of
        true ->
            Module:terminate(Reason, ModuleState);
        false ->
            ok
    end,
    rocksdb:close(State#state.db).


%%====================================================================
%% Internal functions
%%====================================================================

handle_pending_inbound(State) ->
    %% so we need to start at the oldest messages in the inbound queue and
    %% attempt Module:handle_message on each one. If the module returns `defer'
    %% we need to not attempt to deliver any newer messages from that actor.
    %% This function returns when either all actors have hit a defer, or we run
    %% out of messages to examine. If we are successful in handling any inbound
    %% messages during the run, we should loop back to the oldest messages and
    %% try to handle them again, as the module may now be ready to handle them.
    {ok, Iter} = rocksdb:iterator(State#state.db, [{iterate_upper_bound, max_inbound_key()}]),
    Res = rocksdb:iterator_move(Iter, {seek, min_inbound_key()}),
    Deferring = [],
    case find_next_inbound(Res, Iter, Deferring, State) of
        {stop, Timeout, State} ->
            {stop, Timeout, State};
        {ok, State} ->
            %% nothing changed, we're done here
            {ok, State};
        {ok, NewState} ->
            %% we changed something, try handling other deferreds again
            handle_pending_inbound(NewState)
    end.

find_next_inbound({error, _}, Iter, _, State) ->
    ok = rocksdb:iterator_close(Iter),
    {ok, State};
find_next_inbound({ok, <<"i", _/binary>> = Key, <<FromActorID:16/integer, Msg/binary>>}, Iter, Deferring, State) ->
    ct:pal("loop ~p", [FromActorID]),
    case lists:member(FromActorID, Deferring) of
        false ->
            case handle_message(Key, FromActorID, Msg, State) of
                defer ->
                    %% done processing messages from this actor
                    find_next_inbound(rocksdb:iterator_move(Iter, next), Iter,
                                      [FromActorID|Deferring], State);
                {ok, NewState} ->
                    find_next_inbound(rocksdb:iterator_move(Iter, next), Iter,
                                      Deferring, NewState);
                {stop, Timeout, NewState} ->
                    ok = rocksdb:iterator_close(Iter),
                    {stop, Timeout, NewState}
            end;
        true ->
              find_next_inbound(rocksdb:iterator_move(Iter, next), Iter,
                                Deferring, State)
    end;
find_next_inbound({ok, Key, _Value}, Iter, Deferring, State) ->
    %% XXX erlang-rocksb doesn't actually support the iterate_upper_bound option yet so we will see keys past the end of the range
    case Key > max_inbound_key() of
        true ->
            ok = rocksdb:iterator_close(Iter),
            {ok, State};
        false ->
            %% this should not happen
            ct:pal("skipping ~p ~p~n", [Key, _Value]),
            find_next_inbound(rocksdb:iterator_move(Iter, next), Iter,
                              Deferring, State)
    end.


handle_message(Key, FromActorID, Message, State = #state{module=Module, modulestate=ModuleState, db=DB}) ->
    case Module:handle_message(Message, FromActorID, ModuleState) of
        defer ->
            defer;
        {NewModuleState, Actions} ->
            {ok, Batch} = rocksdb:batch(),
            %% write new output messages, update the state and delete the message atomically
            ok = rocksdb:batch_delete(Batch, Key),
            case handle_actions(Actions, Batch, State#state{modulestate=NewModuleState}) of
                {ok, NewState} ->
                    ok = rocksdb:batch_put(Batch, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
                    {ok, NewState#state{modulestate=NewModuleState}};
                {stop, Timeout, NewState} ->
                    ok = rocksdb:batch_put(Batch, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
                    {stop, Timeout, NewState#state{modulestate=NewModuleState}}
            end
    end.

%% write all resulting messages and keys in an atomic batch
handle_actions([], _Batch, State) ->
    {ok, State};
handle_actions([{multicast, Message}|Tail], Batch, State =
               #state{key_count=KeyCount, bitfieldsize=BitfieldSize, id=ID}) ->
    Bitfield = make_bitfield(BitfieldSize, State#state.ids, ID),
    rocksdb:batch_put(Batch, make_outbound_key(KeyCount), <<0:1/integer, Bitfield:BitfieldSize/bits, Message/binary>>),
    %% queue our own copy of the message
    %% we can't handle it here because it's possible it would be out-of-order relative to other inbound
    %% queued messages from ourself that got deferred
    rocksdb:batch_put(Batch, make_inbound_key(KeyCount+1), <<ID:16/integer, Message/binary>>),
    handle_actions(Tail, Batch, State#state{key_count=KeyCount+2});
handle_actions([{unicast, ToActorID, Message}|Tail], Batch, State = #state{key_count=KeyCount}) ->
    rocksdb:batch_put(Batch, make_outbound_key(KeyCount), <<1:1/integer, ToActorID:15/integer, Message/binary>>),
    handle_actions(Tail, Batch, State#state{key_count=KeyCount+1});
handle_actions([{stop, Timeout}|_Tail], _Batch, State) ->
    {stop, Timeout, State}.

make_bitfield(BitfieldSize, Actors, Actor) ->
    Bits = << begin
                  case A of
                      Actor ->
                          <<0:1/integer>>;
                      _ ->
                          <<1:1/integer>>
                  end
              end || A <- Actors >>,
    Padding = << <<0:1/integer>> || _ <- lists:seq(0, BitfieldSize -
                                                        length(Actors)) >>,
    <<Bits:(length(Actors))/bits, Padding:(BitfieldSize -
                                           length(Actors))/bits>>.

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
    {ok, InIter} = rocksdb:iterator(DB, [{iterate_upper_bound, max_inbound_key()}]),
    %% XXX iterate_upper_bound doesn't work, so we can't use it.
    %% instead we seek to the last possible key, and if that is not present,
    %% seek to the previous key
    MaxInbound = case rocksdb:iterator_move(InIter, max_inbound_key()) of
        {ok, <<"i", InNum:10/binary>>, _} ->
            list_to_integer(binary_to_list(InNum));
        _ ->
            case rocksdb:iterator_move(InIter, prev) of
                {ok, <<"i", InNum:10/binary>>, _} ->
                    list_to_integer(binary_to_list(InNum));
                _ ->
                    0
            end
    end,
    rocksdb:iterator_close(InIter),
    {ok, OutIter} = rocksdb:iterator(DB, [{iterate_upper_bound, max_outbound_key()}]),
    MaxOutbound = case rocksdb:iterator_move(OutIter, max_outbound_key()) of
        {ok, <<"o", OutNum:10/binary>>, _} ->
            list_to_integer(binary_to_list(OutNum));
        _ ->
            case rocksdb:iterator_move(OutIter, prev) of
                {ok, <<"o", OutNum:10/binary>>, _} ->
                    list_to_integer(binary_to_list(OutNum));
                _ ->
                    0
            end
    end,
    rocksdb:iterator_close(OutIter),
    ct:pal("last key ~p ~p", [MaxInbound, MaxOutbound]),
    max(MaxInbound, MaxOutbound).

%% iterate the outbound messages until we find one for this ActorID
find_next_outbound(ActorID, StartKey, DB, ActorCount) ->
    {ok, Iter} = rocksdb:iterator(DB, [{iterate_upper_bound, max_outbound_key()}]),
    Res = rocksdb:iterator_move(Iter, StartKey),
    find_next_outbound_(ActorID, Res, Iter, ActorCount).

find_next_outbound_(_ActorId, {error, _}, Iter, _ActorCount) ->
    rocksdb:iterator_close(Iter),
    not_found;
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<1:1/integer, ActorID:15/integer, Value/binary>>}, Iter, _ActorCount) ->
    %% unicast message for this actor
    rocksdb:iterator_close(Iter),
    {Key, Value};
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<0:1/integer, Tail/bits>>}, Iter, ActorCount) ->
    <<ActorMask:ActorCount/integer-unsigned-big, Value/binary>> = Tail,
    case ActorMask band (1 bsl (ActorCount - ActorID)) of
        0 ->
            %% not for us, keep looking
            find_next_outbound_(ActorID, rocksdb:iterator_move(Iter, next), Iter, ActorCount);
        _ ->
            %% multicast message with the high bit set for this actor
            rocksdb:iterator_close(Iter),
            {Key, Value}
    end;
find_next_outbound_(_ActorID, {ok, _Key, _Value}, Iter, _ActorCount) ->
    %% we hit the upper bound of the outbound messages
    rocksdb:iterator_close(Iter),
    not_found.

min_inbound_key() ->
    <<"i0000000000">>.

max_inbound_key() ->
    <<"i4294967296">>.

min_outbound_key() ->
    <<"o0000000000">>.

max_outbound_key() ->
    <<"o4294967296">>.

make_inbound_key(KeyCount) ->
    ct:pal("make inbound key ~p", [KeyCount]),
    list_to_binary(io_lib:format("i~10..0b", [KeyCount])).

make_outbound_key(KeyCount) ->
    list_to_binary(io_lib:format("o~10..0b", [KeyCount])).
