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
%% * <<"stored_module_state">> - this key stores the latest serialized state of the
%%                        callback module's state. It is only read back from disk on
%%                        recovery. This key is overwritten every time the module
%%                        handles a message or an event.
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
%%  0. Once all the bits have been set to 0, the key can be deleted. The bitmask
%%  is stored least significant bit first and is padded to be a multiple of 8
%%  (along with the leading 0 bit) so the message is always byte aligned.
%%
%%  Epochs
%%  Relcast has the notion of epochs for protocols that have the property of "if
%%  one honest node can complete the round, all nodes can". When appropriate,
%%  the module can return the 'new_epoch' which moves the current epoch to the
%%  previous epoch and destroys the old previous epoch, if any. This means only
%%  the last two epochs need to be kept. Messages older than that are, by
%%  definition, not necessary for the protocol to continue advancing and are
%%  discarded.
%%
%%  To do this, relcast uses rocksdb's column families feature. On initial
%%  startup it creates the column family "epoch0000000000". Each 'new_epoch'
%%  action the epoch counter is incremented by one. 2^32 epochs are allowed
%%  before the counter wraps. Don't have 4 billion epochs, please.

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

-type actions() :: [ Message :: message() |
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
          epoch = 0 :: non_neg_integer(),
          bitfieldsize :: pos_integer(),
          prev_cf :: undefined | rocksdb:cf_handle(),
          active_cf :: rocksdb:cf_handle()
         }).

-record(iterator, {
          db :: rocksdb:db_handle(),
          iterator :: rocksdb:itr_handle(),
          args :: list(),
          cf :: rocksdb:cf_handle(),
          next_cf :: undefined | rocksdb:cf_handle(),
          message_type :: inbound | outbound
         }).

-export([start/5, command/2, deliver/3, take/2, ack/3, stop/2, status/1]).

-spec start(pos_integer(), [pos_integer(),...], atom(), list(), list()) -> error | {ok, #state{}} | {stop, pos_integer(), #state{}}.
start(ActorID, ActorIDs, Module, Arguments, RelcastOptions) ->
    DataDir = proplists:get_value(data_dir, RelcastOptions),
    DBOptions = db_options(length(ActorIDs)),
    ColumnFamilies = case rocksdb:list_column_families(DataDir, DBOptions) of
                         {ok, CFs0} ->
                             CFs = lists:sort(CFs0) -- ["default"],
                             case length(CFs) of
                                 0 ->
                                     %% we need to create epoch 0
                                     [];
                                 _ ->
                                     %% we should prune all but the last two
                                     CFs
                             end;
                         {error, _} ->
                             %% Assume the database doesn't exist yet, if we can't open it we will fail later
                             []
                     end,
    {ok, DB, [_DefaultCF|CFHs]} = rocksdb:open_with_cf(DataDir, [{create_if_missing, true}], [ {CF, DBOptions} || CF <- ["default"|ColumnFamilies] ]),
    %% check if we have some to prune
    %% delete all but the two newest *contiguous* column families
    {Epoch, PrevCF, ActiveCF} = case lists:reverse(ColumnFamilies) of
                             [] ->
                                 %% no column families, create epoch 0
                                 {ok, FirstCF} = rocksdb:create_column_family(DB, make_column_family_name(0), DBOptions),
                                 {0, undefined, FirstCF};
                             [JustOne] ->
                                 %% only a single column family, no need to prune
                                 {cf_to_epoch(JustOne), undefined, hd(CFHs)};
                             [Last, SecondToLast|_Tail] ->
                                 %% check if the last two are contiguous
                                 case cf_to_epoch(Last) - cf_to_epoch(SecondToLast) == 1 of
                                     true ->
                                         %% the last two are contiguous, delete all but those
                                         CFsToDelete = lists:sublist(CFHs, 1, length(CFHs) - 2),
                                         [ ok = rocksdb:drop_column_family(CFH) || CFH <- CFsToDelete ],
                                         list_to_tuple([cf_to_epoch(Last)|lists:sublist(CFHs, length(CFHs) + 1 - 2, 2)]);
                                     false ->
                                         %% the last two are non contiguous, gotta prune all but the last one
                                         CFsToDelete = lists:sublist(CFHs, 1, length(CFHs) - 1),
                                         [ ok = rocksdb:drop_column_family(CFH) || CFH <- CFsToDelete ],
                                         {cf_to_epoch(Last), undefined, hd(lists:sublist(CFHs, length(CFHs) + 1 - 1, 1))}
                                 end
                         end,
    case erlang:apply(Module, init, Arguments) of
        {ok, ModuleState0} ->
            ModuleState = case rocksdb:get(DB, ActiveCF, <<"stored_module_state">>, []) of
                              {ok, SerializedModuleState} ->
                                  OldModuleState = Module:deserialize(SerializedModuleState),
                                  {ok, RestoredModuleState} = Module:restore(OldModuleState, ModuleState0),
                                  RestoredModuleState;
                              not_found ->
                                  %% TODO there might be a chance we crashed between creating the next column family
                                  %% and writing the module state into it, check for this.
                                  ModuleState0
                          end,
            LastKey = get_last_key(DB, ActiveCF),
            BitFieldSize = round_to_nearest_byte(length(ActorIDs)) - 1, %% one bit for unicast/multicast
            %% try to deliver any old queued inbound messages
            handle_pending_inbound(#state{module=Module, id=ActorID,
                                          prev_cf = PrevCF,
                                          active_cf = ActiveCF,
                                          ids=ActorIDs,
                                          modulestate=ModuleState, db=DB,
                                          key_count=LastKey+1,
                                          epoch=Epoch,
                                          bitfieldsize=BitFieldSize});
        _ ->
            error
    end.

-spec command(any(), #state{}) -> {any(), #state{}} | {stop, any(), pos_integer(), #state{}}.
command(Message, State = #state{module=Module, modulestate=ModuleState, db=DB}) ->
    {reply, Reply, Actions, NewModuleState} = Module:handle_command(Message, ModuleState),
    {ok, Batch} = rocksdb:batch(),
    %% write new output messages & update the state atomically
    case handle_actions(Actions, Batch, State#state{modulestate=NewModuleState}) of
        {ok, NewState} ->
            ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
            case handle_pending_inbound(NewState) of
                {ok, NewerState} ->
                    {Reply, NewerState};
                {stop, Timeout, NewerState} ->
                    {stop, Reply, Timeout, NewerState}
            end;
        {stop, Timeout, NewState} ->
            ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
            {stop, Reply, Timeout, NewState}
    end.

-spec deliver(binary(), pos_integer(), #state{}) -> {ok, #state{}} | {stop, pos_integer(), #state{}}.
deliver(Message, FromActorID, State0 = #state{key_count=KeyCount, db=DB, active_cf=CF}) ->
    Key = make_inbound_key(KeyCount), %% some kind of predictable, monotonic key
    ok = rocksdb:put(DB, CF, Key, <<FromActorID:16/integer, Message/binary>>, [{sync, true}]),
    State = State0#state{key_count=KeyCount+1},
    case handle_pending_inbound(State) of
        {ok, NewState} ->
            case rocksdb:get(DB, CF, Key, []) of
                {ok, <<FromActorID:16/integer, Message/binary>>} ->
                    %% key is still there, we deferred it
                    {defer, NewState};
                _ ->
                    {ok, NewState}
            end;
        {stop, Timeout, NewState} ->
            {stop, Timeout, NewState}
    end.

-spec take(pos_integer, #state{}) -> not_found | {ok, reference(), binary(), #state{}}.
take(ForActorID, State = #state{bitfieldsize=BitfieldSize, db=DB}) ->
    %% we need to find the first "unacked" message for this actor
    %% we should remember the last acked message for this actor ID and start there
    %% check if there's a pending ACK and use that to find the "last" key, if present
    case maps:get(ForActorID, State#state.pending_acks, undefined) of
        {Ref, CF, Key} ->
            case rocksdb:get(DB, CF, Key, []) of
                {ok, <<1:1/integer, ForActorID:15/integer, Value/binary>>} ->
                    {ok, Ref, Value, State};
                {ok, <<0:1/integer, _:(BitfieldSize)/integer, Value/binary>>} ->
                    {ok, Ref, Value, State};
                not_found ->
                    %% something strange is happening, try again
                    take(ForActorID, State#state{pending_acks=maps:remove(ForActorID, State#state.pending_acks)})
            end;
        _ ->
            {CF, StartKey} = maps:get(ForActorID, State#state.last_sent, {prev_cf(State), min_outbound_key()}), %% default to the "first" key"
            %% iterate until we find a key for this actor
            case find_next_outbound(ForActorID, CF, StartKey, State, State#state.bitfieldsize) of
                {Key, CF2, Msg} ->
                    Ref = make_ref(),
                    {ok, Ref, Msg, State#state{pending_acks=maps:put(ForActorID, {Ref, CF2, Key}, State#state.pending_acks)}};
                not_found ->
                    not_found
            end
    end.

-spec ack(pos_integer(), reference(), #state{}) -> {ok, #state{}}.
ack(FromActorID, Ref, State = #state{bitfieldsize=BitfieldSize, db=DB}) ->
    case maps:get(FromActorID, State#state.pending_acks, undefined) of
        {Ref, CF, Key} ->
            case rocksdb:get(DB, CF, Key, []) of
                {ok, <<1:1/integer, FromActorID:15/integer, _Value/binary>>} ->
                    %% unicast message, fine to delete now
                    ok = rocksdb:delete(DB, CF, Key, [{sync, true}]);
                {ok, <<0:1/integer, SentTo:(BitfieldSize)/integer, _Value/binary>>} ->
                    Padding = BitfieldSize - length(State#state.ids),
                    Bit = length(State#state.ids) - FromActorID,
                    %% multicast message, see if all the bits have gone 0
                    case (SentTo bsr Padding) bxor (1 bsl Bit) of
                        0 ->
                            %% time to delete
                            ok = rocksdb:delete(DB, CF, Key, [{sync, true}]);
                        _Remaining ->
                            %% flip the bit for this actor
                            ActorIDStr = io_lib:format("-~b", [FromActorID]),
                            ok = rocksdb:merge(DB, CF, Key, list_to_binary(ActorIDStr), [{sync, true}])
                    end;
                not_found ->
                    %% something strange is happening
                    ok
            end,
            NewPending = maps:remove(FromActorID, State#state.pending_acks),
            {ok, State#state{pending_acks=NewPending, last_sent=maps:put(FromActorID, {CF, Key}, State#state.last_sent)}};
        _ ->
            {ok, State}
    end.

-spec stop(any(), #state{}) -> ok.
stop(Reason, State = #state{module=Module, modulestate=ModuleState})->
    case erlang:function_exported(Module, terminate, 2) of
        true ->
            Module:terminate(Reason, ModuleState);
        false ->
            ok
    end,
    rocksdb:close(State#state.db).

-spec status(#state{}) -> {ModuleState :: any(), InboundQueue ::
                           [{pos_integer(), binary()}], OutboundQueue ::
                           #{pos_integer() => [binary()]}}.
status(State = #state{modulestate=ModuleState}) ->
    {ok, Iter} = cf_iterator(State, prev_cf(State), both, [{iterate_upper_bound, max_outbound_key()}]),
    {InboundQueue, OutboundQueue} = build_status(cf_iterator_move(Iter, {seek, min_inbound_key()}), Iter, State#state.bitfieldsize, [], #{}),
    {ModuleState, InboundQueue, OutboundQueue}.

%%====================================================================
%% Internal functions
%%====================================================================

-spec handle_pending_inbound(#state{}) -> {stop, pos_integer(), #state{}} | {ok, #state{}}.
handle_pending_inbound(State) ->
    %% so we need to start at the oldest messages in the inbound queue and
    %% attempt Module:handle_message on each one. If the module returns `defer'
    %% we need to not attempt to deliver any newer messages from that actor.
    %% This function returns when either all actors have hit a defer, or we run
    %% out of messages to examine. If we are successful in handling any inbound
    %% messages during the run, we should loop back to the oldest messages and
    %% try to handle them again, as the module may now be ready to handle them.
    {ok, Iter} = cf_iterator(State, prev_cf(State), inbound, [{iterate_upper_bound, max_inbound_key()}]),
    Res = cf_iterator_move(Iter, {seek, min_inbound_key()}),
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
    ok = cf_iterator_close(Iter),
    {ok, State};
find_next_inbound({ok, <<"i", _/binary>> = Key, <<FromActorID:16/integer, Msg/binary>>}, Iter, Deferring, State) ->
    case lists:member(FromActorID, Deferring) of
        false ->
            case handle_message(Key, cf_iterator_id(Iter), FromActorID, Msg, State) of
                defer ->
                    %% done processing messages from this actor
                    find_next_inbound(cf_iterator_move(Iter, next), Iter,
                                      [FromActorID|Deferring], State);
                {ok, NewState} ->
                    %% refresh the iterator so it sees any new keys we wrote
                    cf_iterator_refresh(Iter),
                    find_next_inbound(cf_iterator_move(Iter, next), Iter,
                                      Deferring, NewState);
                {stop, Timeout, NewState} ->
                    ok = cf_iterator_close(Iter),
                    {stop, Timeout, NewState}
            end;
        true ->
              find_next_inbound(cf_iterator_move(Iter, next), Iter,
                                Deferring, State)
    end;
find_next_inbound({ok, Key, _Value}, Iter, Deferring, State) ->
    %% XXX erlang-rocksb doesn't actually support the iterate_upper_bound option yet so we will see keys past the end of the range
    case Key > max_inbound_key() of
        true ->
            ok = cf_iterator_close(Iter),
            {ok, State};
        false ->
            %% this should not happen
            find_next_inbound(cf_iterator_move(Iter, next), Iter,
                              Deferring, State)
    end.


handle_message(Key, CF, FromActorID, Message, State = #state{module=Module, modulestate=ModuleState, db=DB}) ->
    case Module:handle_message(Message, FromActorID, ModuleState) of
        defer ->
            defer;
        {NewModuleState, Actions} ->
            {ok, Batch} = rocksdb:batch(),
            %% write new output messages, update the state and delete the message atomically
            ok = rocksdb:batch_delete(Batch, CF, Key),
            case handle_actions(Actions, Batch, State#state{modulestate=NewModuleState}) of
                {ok, NewState} ->
                    ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
                    {ok, NewState};
                {stop, Timeout, NewState} ->
                    ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
                    {stop, Timeout, NewState}
            end
    end.

%% write all resulting messages and keys in an atomic batch
handle_actions([], _Batch, State) ->
    {ok, State};
handle_actions([new_epoch|Tail], Batch, State) ->
    {ok, NewCF} = rocksdb:create_column_family(State#state.db, make_column_family_name(State#state.epoch + 1), db_options(length(State#state.ids))),
    case State#state.prev_cf of
        undefined -> ok;
        _ ->
            ok = rocksdb:drop_column_family(State#state.prev_cf)
    end,
    %% when we're done handling actions, we will write the module state (and all subsequent outbound messages from this point on)
    %% into the active CF, which is this new one now
    handle_actions(Tail, Batch, State#state{key_count=0, active_cf=NewCF, prev_cf=State#state.active_cf, epoch=State#state.epoch + 1});
handle_actions([{multicast, Message}|Tail], Batch, State =
               #state{key_count=KeyCount, bitfieldsize=BitfieldSize, id=ID, active_cf=CF}) ->
    Bitfield = make_bitfield(BitfieldSize, State#state.ids, ID),
    rocksdb:batch_put(Batch, CF, make_outbound_key(KeyCount), <<0:1/integer, Bitfield:BitfieldSize/bits, Message/binary>>),
    %% queue our own copy of the message
    %% we can't handle it here because it's possible it would be out-of-order relative to other inbound
    %% queued messages from ourself that got deferred
    rocksdb:batch_put(Batch, CF, make_inbound_key(KeyCount+1), <<ID:16/integer, Message/binary>>),
    handle_actions(Tail, Batch, State#state{key_count=KeyCount+2});
handle_actions([{unicast, ToActorID, Message}|Tail], Batch, State = #state{key_count=KeyCount, active_cf=CF}) ->
    rocksdb:batch_put(Batch, CF, make_outbound_key(KeyCount), <<1:1/integer, ToActorID:15/integer, Message/binary>>),
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
get_last_key(DB, CF) ->
    {ok, InIter} = rocksdb:iterator(DB, CF, [{iterate_upper_bound, max_inbound_key()}]),
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
    {ok, OutIter} = rocksdb:iterator(DB, CF, [{iterate_upper_bound, max_outbound_key()}]),
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
    max(MaxInbound, MaxOutbound).

%% iterate the outbound messages until we find one for this ActorID
find_next_outbound(ActorID, CF, StartKey, State, ActorCount) ->
    {ok, Iter} = cf_iterator(State, CF, outbound, [{iterate_upper_bound, max_outbound_key()}]),
    Res = cf_iterator_move(Iter, StartKey),
    find_next_outbound_(ActorID, Res, Iter, ActorCount).

find_next_outbound_(_ActorId, {error, _}, Iter, _ActorCount) ->
    cf_iterator_close(Iter),
    not_found;
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<1:1/integer, ActorID:15/integer, Value/binary>>}, Iter, _ActorCount) ->
    %% unicast message for this actor
    CF = cf_iterator_id(Iter),
    cf_iterator_close(Iter),
    {Key, CF, Value};
find_next_outbound_(ActorID, {ok, <<"o", _/binary>>, <<1:1/integer, _/bits>>}, Iter, ActorCount) ->
    %% unicast message for someone else
    find_next_outbound_(ActorID, cf_iterator_move(Iter, next), Iter, ActorCount);
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<0:1/integer, Tail/bits>>}, Iter, ActorCount) ->
    <<ActorMask:ActorCount/integer-unsigned-big, Value/binary>> = Tail,
    case ActorMask band (1 bsl (ActorCount - ActorID)) of
        0 ->
            %% not for us, keep looking
            find_next_outbound_(ActorID, cf_iterator_move(Iter, next), Iter, ActorCount);
        _ ->
            %% multicast message with the high bit set for this actor
            CF = cf_iterator_id(Iter),
            cf_iterator_close(Iter),
            {Key, CF, Value}
    end;
find_next_outbound_(_ActorID, {ok, _Key, _Value}, Iter, _ActorCount) ->
    %% we hit the upper bound of the outbound messages
    cf_iterator_close(Iter),
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
    list_to_binary(io_lib:format("i~10..0b", [KeyCount])).

make_outbound_key(KeyCount) ->
    list_to_binary(io_lib:format("o~10..0b", [KeyCount])).

make_column_family_name(EpochCount) ->
    lists:flatten(io_lib:format("epoch~10..0b", [EpochCount])).

cf_to_epoch([$e, $p, $o, $c, $h|EpochString]) ->
    list_to_integer(EpochString).

prev_cf(State) ->
    case State#state.prev_cf of
        undefined ->
            State#state.active_cf;
        CF ->
            CF
    end.

build_status({error, _}, Iter, _BFS, InboundQueue, OutboundQueue) ->
    cf_iterator_close(Iter),
    {lists:reverse(InboundQueue), maps:map(fun(_K, V) -> lists:reverse(V) end, OutboundQueue)};
build_status({ok, <<"i", _/binary>>, <<FromActorID:16/integer, Msg/binary>>}, Iter, BFS, InboundQueue, OutboundQueue) ->
    build_status(cf_iterator_move(Iter, next), Iter, BFS, [{FromActorID, Msg}|InboundQueue], OutboundQueue);
build_status({ok, <<"o", _/binary>>, <<1:1/integer, ActorID:15/integer, Value/binary>>}, Iter, BFS, InboundQueue, OutboundQueue) ->
    %% unicast message
    build_status(cf_iterator_move(Iter, next), Iter, BFS, InboundQueue, prepend_message([ActorID], Value, OutboundQueue));
build_status({ok, <<"o", _/binary>>, <<0:1/integer, Tail/bits>>}, Iter, BFS, InboundQueue, OutboundQueue) ->
    <<ActorMask:BFS/bits, Value/binary>> = Tail,
    ActorIDs = actor_list(ActorMask, 1, []),
    build_status(cf_iterator_move(Iter, next), Iter, BFS, InboundQueue, prepend_message(ActorIDs, Value, OutboundQueue));
build_status({ok, _Key, _Value}, Iter, BFS, InboundQueue, OutboundQueue) ->
    build_status(cf_iterator_move(Iter, next), Iter, BFS, InboundQueue, OutboundQueue).

prepend_message(Actors, Message, Map) ->
    ExtraMap = [{K, []} || K <- (Actors -- maps:keys(Map))],
    maps:map(fun(K, V) ->
                     case lists:member(K, Actors) of
                         true ->
                             [Message|V];
                         false ->
                             V
                     end
             end, maps:merge(maps:from_list(ExtraMap), Map)).

actor_list(<<>>, _, List) ->
    List;
actor_list(<<1:1/integer, Tail/bits>>, I, List) ->
    actor_list(Tail, I+1, [I|List]);
actor_list(<<0:1/integer, Tail/bits>>, I, List) ->
    actor_list(Tail, I+1, List).

%% wrapper around rocksdb iterators that will iterate across column families.
%% Requirements to use this:
%% * you only iterate forwards
%% * you only iterate inbound/outbound messages
%% * jumps across column families will start in the next
%%   column family at the first key inbound/outbound key
-spec cf_iterator(#state{}, rocksdb:cf_handle(), inbound | outbound | both, list()) -> {ok, reference()}.
cf_iterator(State, CF, MsgType, Args) when MsgType == inbound; MsgType == outbound; MsgType == both ->
    Ref = make_ref(),
    {CF, NextCF} = case {State#state.prev_cf, State#state.active_cf} of
                       {undefined, Active} ->
                           %% only one CF
                           {Active, undefined};
                       {CF, Active} ->
                           %% we want to start on the previous CF
                           {CF, Active};
                       {_, CF} ->
                           %% we want to start on the current CF
                           {CF, undefined}
                   end,
    {ok, Iter} = rocksdb:iterator(State#state.db, CF, Args),
    erlang:put(Ref, #iterator{db=State#state.db, iterator=Iter, args=Args, cf=CF, next_cf=NextCF, message_type=MsgType}),
    {ok, Ref}.

-spec cf_iterator_move(reference(), next | {seek, binary()} | binary()) -> {ok, Key::binary(), Value::binary()} | {ok, Key::binary()} | {error, invalid_iterator} | {error, iterator_closed}.
cf_iterator_move(Ref, Args) ->
    Iterator = erlang:get(Ref),
    Type = Iterator#iterator.message_type,
    case rocksdb:iterator_move(Iterator#iterator.iterator, Args) of
        {ok, <<"i", _/binary>>, _Value} = Res when Type == inbound; Type == both ->
            Res;
        {ok, <<"o", _/binary>>, _Value} = Res when Type == outbound; Type == both ->
            Res;
        {error, _} = Res when Iterator#iterator.next_cf == undefined ->
            Res;
        {ok, _Key, _Value} when Iterator#iterator.next_cf == undefined ->
            {error, invalid_iterator};
        _ ->
            %% error or out-of-range key
            %% try the next column family
            ok = rocksdb:iterator_close(Iterator#iterator.iterator),
            {ok, Iter} = rocksdb:iterator(Iterator#iterator.db, Iterator#iterator.next_cf, Iterator#iterator.args),
            erlang:put(Ref, Iterator#iterator{next_cf=undefined, cf=Iterator#iterator.next_cf, iterator=Iter}),
            case Iterator#iterator.message_type of
                inbound ->
                    rocksdb:iterator_move(Iter, min_inbound_key());
                both ->
                    rocksdb:iterator_move(Iter, min_inbound_key());
                outbound ->
                    rocksdb:iterator_move(Iter, min_outbound_key())
            end
    end.

-spec cf_iterator_id(reference()) -> rocksdb:cf_handle().
cf_iterator_id(Ref) ->
    Iterator = erlang:get(Ref),
    Iterator#iterator.cf.

cf_iterator_refresh(Ref) ->
    Iterator = erlang:get(Ref),
    rocksdb:iterator_refresh(Iterator#iterator.iterator).

-spec cf_iterator_close(reference()) -> ok.
cf_iterator_close(Ref) ->
    Iterator = erlang:get(Ref),
    erlang:erase(Ref),
    rocksdb:iterator_close(Iterator#iterator.iterator).
