
%% @doc
%% Relcast's job is ensure a consistent state for consensus protocols. It
%% provides atomic updates to the consensus state, the inbound message queue and
%% the outbound message queue. It does this by speculatively processing inbound messages,
%% serializing them to disk if they can't be handled now, by serializing the new module
%% state and any outbound messages to disk and deleting the inbound message
%% after processing the message. Assuming no disk failures, the Erlang process,
%% the Erlang VM or the host operating system should be able to fail at any time
%% and recover where it left off. All messages have a clear ownership and are not
%% removed until they've been handled or someone else has taken ownership.
%%
%% Relcast does this using 3 kinds of keys
%%
%% * `<<"stored_module_state">>' - this key stores the latest serialized state of the
%%                        callback module's state. It is only read back from disk on
%%                        recovery. This key is overwritten every time the module
%%                        handles a message or an event.
%%
%% * `<<"oXXXXXXXXXX">>'  - an outbound key, representing a message this instance
%%                        wishes to send to another peer.
%%
%% * `<<"iXXXXXXXXXX">>'  - an inbound key, this represents a message arriving
%%                        that has not been handled yet.
%%
%%  The 10 Xs in the inbound and outbound keys represent a strictly monotonic
%%  counter that can hold 2^32 messages. They are prefixed with their direction
%%  so we can efficiently iterate over them independently. The 32 bit integer is
%%  printed in left zero padded decimal so that the keys sort lexiographically.
%%
%%  Inbound values are stored in the form `<<ActorID:16/integer, Value/binary>>'.
%%  Inbound messages are only stored if the handler indicates they cannot be
%%  handled right now, up to a per-actor maximum. Other inbound events are
%%  handled immediately and any new state or new outbound messages are stored to
%%  disk.
%%
%%  Outbound values come in 3 types; unicast, multicast and 'callback'.
%%
%%  Unicast values look like this: `<<1:2/bits, ActorID:14/integer, Value/binary>>'
%%  and are only intended for delivery to a single peer, identified by ActorID.
%%  Once the designated Actor has ACKed the message, the key can be deleted.
%%
%%  Multicast values look like this:
%%  `<<0:2/bits, ActorBitMask:BitmaskSize/integer, Value/binary>>' and are
%%  intended to be delivered to every other actor in the consensus group. Each
%%  time a send to one of the peers is ACKed, the bit for that actor is set to
%%  0. Once all the bits have been set to 0, the key can be deleted. The bitmask
%%  is stored least significant bit first and is padded to be a multiple of 8
%%  (along with the leading 0 bit) so the message is always byte aligned.
%%
%%  Callback values look like this:
%%  `<<2:2/bits, ActorBitMask:BitmaskSize/integer, Value/binary>>' and are very
%%  similar to multicast values with one crucial difference. When relcast finds
%%  that the next message for an actor is a callback message, it invokes
%%  Module:callback_message(ActorID, Message, ModuleState). This call should
%%  produce either the actual binary of the message to send or `none` to indicate
%%  no message should be sent for that actor (in which case the bitfield is cleared
%%  for that actor immediately and the search for the next message for that actor
%%  continues. This message type is useful, for example, if you have a message with
%%  a large common term and some smaller per-user terms that you need to send to
%%  all actors but don't want to store separetely N times. It could also be useful
%%  if you don't know if the message will ever be sent and it involves some expensive
%%  computation or a signature. The module should not be modified as it is not returned.
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

-module(relcast).

%%====================================================================
%% Callback functions
%%====================================================================
-callback init(Arguments :: any()) -> {ok, State :: term()}.
-callback restore(OldState :: term(), NewState :: term()) -> {ok, State :: term()}.
-callback serialize(State :: term()) -> Binary :: binary().
-callback deserialize(Binary :: binary()) -> State :: term().
-callback handle_message(Message :: binary(), ActorId :: pos_integer(), State :: term()) ->
    {NewState :: term(), Actions :: actions()} | defer | ignore.
-callback handle_command(Request :: term(), State :: term()) ->
   {reply, Reply :: term(), Actions :: actions(), NewState :: term()} |
   {reply, Reply :: term(), ignore}. %% when there's no changes, likely just returning information
-callback callback_message(ActorID :: pos_integer(), Message :: binary(), State :: term()) ->
    binary() | none.
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
%% State record
%%====================================================================

-record(state, {
          db :: rocksdb:db_handle(),
          module :: atom(),
          modulestate :: any(),
          id :: pos_integer(),
          ids :: [pos_integer()],
          last_sent = #{} :: #{pos_integer() => {rocksdb:cf_handle(), binary()} | none},
          pending_acks = #{} ::
            #{pos_integer() => [{PendRef :: reference(),
                                 Epoch :: rocksdb:cf_handle(),
                                 Key :: binary(),
                                 Multicast :: boolean()}]},
          in_key_count = 0 :: non_neg_integer(),
          out_key_count = 0 :: non_neg_integer(),
          epoch = 0 :: non_neg_integer(),
          bitfieldsize :: pos_integer(),
          inbound_cf :: rocksdb:cf_handle(),
          prev_cf :: undefined | rocksdb:cf_handle(),
          active_cf :: rocksdb:cf_handle(),
          defers = #{} :: #{pos_integer() => [binary()]},
          seq_map = #{} :: #{pos_integer() => pos_integer()}
         }).

-record(iterator, {
          db :: rocksdb:db_handle(),
          iterator :: rocksdb:itr_handle(),
          args :: list(),
          cf :: rocksdb:cf_handle(),
          next_cf :: undefined | rocksdb:cf_handle(),
          message_type :: inbound | outbound | both
         }).

-type relcast_state() :: #state{}.
-type status() :: {ModuleState :: any(),
                   InboundQueue :: [{pos_integer(), binary()}],
                   OutboundQueue :: #{pos_integer() => [binary()]}}.
-export_type([relcast_state/0, status/0]).

-export([
         start/5,
         command/2,
         deliver/3,
         take/2, take/3,
         reset_actor/2,
         in_flight/2,
         peek/2,
         ack/3, multi_ack/3,
         stop/2,
         status/1
        ]).

%% @doc Start a relcast instance. Starts a relcast instance for the actor
%% `ActorID' in the group of `ActorIDs' using the callback module `Module'
%% initialized with `Arguments'. `RelcastOptions' contains configuration options
%% around the relcast itself, for example the data directory.
-spec start(pos_integer(), [pos_integer(),...], atom(), list(), list()) -> error | {ok, relcast_state()} | {stop, pos_integer(), relcast_state()}.
start(ActorID, ActorIDs, Module, Arguments, RelcastOptions) ->
    DataDir = proplists:get_value(data_dir, RelcastOptions),
    DBOptions = db_options(length(ActorIDs)),
    {ColumnFamilies, HasInbound} = case rocksdb:list_column_families(DataDir, DBOptions) of
                                       {ok, CFs0} ->
                                           CFs = lists:sort(CFs0) -- ["default"],
                                           HI = lists:member("Inbound", CFs0),
                                           case length(CFs) of
                                               0 ->
                                                   %% we need to create epoch 0
                                                   {[], HI};
                                               _ ->
                                                   %% we should prune all but the last two
                                                   {CFs, HI}
                                           end;
                                       {error, _} ->
                                           %% Assume the database doesn't exist yet, if we can't open it we will fail later
                                           {[], false}
                                   end,
    {ok, DB, [_DefaultCF|CFHs0]} = rocksdb:open_with_cf(DataDir, [{create_if_missing, true}], [ {CF, DBOptions} || CF <- ["default"|ColumnFamilies] ]),
    {InboundCF, CFHs} = case HasInbound of
                            false ->
                                {ok, ICF} = rocksdb:create_column_family(DB, "Inbound", DBOptions),
                                {ICF, CFHs0};
                            true ->
                                {hd(CFHs0), tl(CFHs0)}
                        end,
    %% check if we have some to prune
    %% delete all but the two newest *contiguous* column families
    {Epoch, PrevCF, ActiveCF} = case lists:reverse(ColumnFamilies -- ["Inbound"]) of
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
                                         [ ok = rocksdb:destroy_column_family(CFH) || CFH <- CFsToDelete ],
                                         list_to_tuple([cf_to_epoch(Last)|lists:sublist(CFHs, length(CFHs) + 1 - 2, 2)]);
                                     false ->
                                         %% the last two are non contiguous, gotta prune all but the last one
                                         CFsToDelete = lists:sublist(CFHs, 1, length(CFHs) - 1),
                                         [ ok = rocksdb:drop_column_family(CFH) || CFH <- CFsToDelete ],
                                         [ ok = rocksdb:destroy_column_family(CFH) || CFH <- CFsToDelete ],
                                         {cf_to_epoch(Last), undefined, hd(lists:sublist(CFHs, length(CFHs) + 1 - 1, 1))}
                                 end
                         end,
    case Module:init(Arguments) of
        {ok, ModuleState0} ->
            ModuleState = case rocksdb:get(DB, ActiveCF, <<"stored_module_state">>, []) of
                              {ok, SerializedModuleState} ->
                                  OldModuleState = Module:deserialize(SerializedModuleState),
                                  {ok, RestoredModuleState} = Module:restore(OldModuleState, ModuleState0),
                                  RestoredModuleState;
                              not_found when PrevCF /= undefined ->
                                  %% there might be a chance we crashed between creating the next column family
                                  %% and writing the module state into it, so we check for this:
                                  case rocksdb:get(DB, PrevCF, <<"stored_module_state">>, []) of
                                      {ok, SerializedModuleState} ->
                                          OldModuleState = Module:deserialize(SerializedModuleState),
                                          {ok, RestoredModuleState} = Module:restore(OldModuleState, ModuleState0),
                                          %% write the module state to the new epoch so it's here the next time
                                          ok = rocksdb:put(DB, ActiveCF, <<"stored_module_state">>, Module:serialize(RestoredModuleState), [{sync, true}]),
                                          RestoredModuleState;
                                      not_found ->
                                          ModuleState0
                                  end;
                              not_found ->
                                  ModuleState0
                          end,
            LastKeyIn = get_last_key_in(DB, InboundCF),
            LastKeyOut = get_last_key_out(DB, ActiveCF),
            BitFieldSize = round_to_nearest_byte(length(ActorIDs) + 2) - 2, %% two bits for unicast/multicast
            State = #state{module=Module, id=ActorID,
                           inbound_cf = InboundCF,
                           prev_cf = PrevCF,
                           active_cf = ActiveCF,
                           ids=ActorIDs,
                           modulestate=ModuleState, db=DB,
                           out_key_count=LastKeyOut+1,
                           in_key_count=LastKeyIn+1,
                           epoch=Epoch,
                           bitfieldsize=BitFieldSize},
            {ok, Iter} = rocksdb:iterator(State#state.db, InboundCF, [{iterate_upper_bound, max_inbound_key()}]),
            Defers = build_defer_list(rocksdb:iterator_move(Iter, {seek, min_inbound_key()}), Iter, InboundCF, #{}),
            %% try to deliver any old queued inbound messages
            {ok, Batch} = rocksdb:batch(),
            {ok, NewState} = handle_pending_inbound(Batch, State#state{defers=Defers}),
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
            {ok, NewState};
        _ ->
            error
    end.

%% @doc Send a command to the relcast callback module. Commands are distinct
%% from messages as they do not originate from another actor in the relcast
%% group. Commands are dispatched to `Module':handle_command and can simply
%% return information via `{reply, Reply, ignore}' or update the callback
%% module's state or send messages via `{reply, Reply, Actions, NewModuleState}'.
-spec command(any(), relcast_state()) -> {any(), relcast_state()} | {stop, any(), pos_integer(), relcast_state()}.
command(Message, State = #state{module=Module, modulestate=ModuleState, db=DB}) ->
    case Module:handle_command(Message, ModuleState) of
        {reply, Reply, ignore} ->
            %% just returning information
            {Reply, State};
        {reply, Reply, Actions, NewModuleState} ->
            {ok, Batch} = rocksdb:batch(),
            %% write new output messages & update the state atomically
            Result = case handle_actions(Actions, Batch, State#state{modulestate=NewModuleState}) of
                         {ok, NewState} ->
                             ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                             case handle_pending_inbound(Batch, NewState) of
                                 {ok, NewerState} ->
                                     {Reply, NewerState};
                                 {stop, Timeout, NewerState} ->
                                     {stop, Reply, Timeout, NewerState}
                             end;
                         {stop, Timeout, NewState} ->
                             ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                             {stop, Reply, Timeout, NewState}
                     end,
            ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
            Result
    end.

%% @doc Deliver a message from another actor to the relcast instance. `Message'
%% from `FromActorID' is submitted via `Module':handle_message. Depending on the
%% result of this, the message is either consumed immediately, deferred for
%% later, or this function returns `full' to indicate it cannot absorb any more
%% deferred messages from this Actor.
-spec deliver(binary(), pos_integer(), relcast_state()) -> {ok, relcast_state()} | {stop, pos_integer(), relcast_state()} | full.
deliver(Message, FromActorID, State = #state{in_key_count=KeyCount, db=DB, defers=Defers}) ->
    {ok, Batch} = rocksdb:batch(),
    Result = case handle_message(undefined, undefined, FromActorID, Message, Batch, State) of
                 {ok, NewState} ->
                     %% something happened, evaluate if we can handle any other blocked messages
                     case length(maps:keys(Defers)) of
                         0 ->
                             %% no active defers, no queued inbound messages to evaluate
                             {ok, NewState};
                         _ ->
                             case handle_pending_inbound(Batch, NewState) of
                                 {ok, NewerState} ->
                                     {ok, NewerState};
                                 {stop, Timeout, NewerState} ->
                                     {stop, Timeout, NewerState}
                             end
                     end;
                 {stop, Timeout, NewState} ->
                     {stop, Timeout, NewState};
                 ignore ->
                     {ok, State};
                 defer ->
                     DefersForThisActor = maps:get(FromActorID, Defers, []),
                     MaxDefers = application:get_env(relcast, max_defers, 100),
                     case DefersForThisActor of
                         N when length(N) < MaxDefers ->
                             Key = make_inbound_key(KeyCount), %% some kind of predictable, monotonic key
                             ok = rocksdb:batch_put(Batch, State#state.inbound_cf, Key, <<FromActorID:16/integer, Message/binary>>),
                             {ok, State#state{in_key_count=KeyCount+1, defers=maps:put(FromActorID, [Key|N], Defers)}};
                         _ ->
                             %% sorry buddy, no room on the couch
                             full
                     end
             end,
    ok = rocksdb:write_batch(DB, Batch, [{sync, true}]),
    Result.

%% default to false for ergonomics
take(ID, State) ->
    take(ID, State, false).

%% @doc Get the next message this relcast has queued outbound for `ForActorID'.
%% Once this message has been delivered to its destination, and acknowledged,
%% `ack()' should be called with reference associated with the message.
%% Subsequent calls to `take()' without any intervening acks will return more
%% messages up to the pipeline depth, thereafter it will return the
%% `pipeline_full' tuple.  In the case where the client code has lost its
%% connection, it should call `take/3' with `true', which will reset the pending
%% acks state and reissue the oldest unacked message in case all of the unacked
%% messages were lost in flight.
-spec take(pos_integer(), relcast_state(), boolean()) ->
                  {not_found, relcast_state()} |
                  {pipeine_full, relcast_state()} |
                  {ok, non_neg_integer(), binary(), relcast_state()}.
take(ForActorID, State, true) ->
    {ok, NewState} = reset_actor(ForActorID, State),
    take(ForActorID, NewState, false);
take(ForActorID, State = #state{pending_acks = Pending}, _) ->
    %% we need to find the first "unacked" message for this actor
    %% we should remember the last acked message for this actor ID and start there
    %% check if there's a pending ACK and use that to find the "last" key, if present
    PipelineDepth = application:get_env(relcast, pipeline_depth, 20),
    case maps:get(ForActorID, Pending, []) of
        Pends when length(Pends) >= PipelineDepth ->
            {pipeline_full, State};
        Pends when Pends /= [] ->
            case lists:last(Pends) of
                {_Seq, CF, Key, _Multicast} when CF == State#state.active_cf; CF == State#state.prev_cf ->
                    %% iterate until we find a key for this actor
                    case find_next_outbound(ForActorID, CF, Key, State, false) of
                        {not_found, LastKey, CF2} ->
                            {not_found, State#state{last_sent = maps:put(ForActorID, {CF2, LastKey}, State#state.last_sent)}};
                        {Key2, CF2, Msg, Multicast} ->
                            {Seq2, State2} = make_seq(ForActorID, State),
                            Pends1 = Pends ++ [{Seq2, CF2, Key2, Multicast}],
                            {ok, Seq2, Msg,
                             State2#state{pending_acks = maps:put(ForActorID, Pends1, Pending)}};
                        not_found ->
                            {not_found, State#state{last_sent=maps:put(ForActorID, none, State#state.last_sent)}}
                    end;
                %% all our pends are for a stale epoch, clean them out
                _ ->
                    %% adding true here resets all the pending acks before retrying
                    take(ForActorID, State, true)
            end;
        _ ->
            %% default to the "first" key"
            case maps:get(ForActorID, State#state.last_sent, {prev_cf(State), min_outbound_key()}) of
                none ->
                    %% we *know* there's nothing pending for this actor
                    {not_found, State};
                {CF0, StartKey0} ->
                    %% check if the column family is still valid
                    {CF, StartKey} = case CF0 == State#state.active_cf orelse CF0 == State#state.prev_cf of
                                         true ->
                                             {CF0, StartKey0};
                                         false ->
                                             %% reset the start key as well
                                             {State#state.prev_cf, min_outbound_key()}
                                     end,
                    %% iterate until we find a key for this actor
                    case find_next_outbound(ForActorID, CF, StartKey, State) of
                        {not_found, LastKey, CF2} ->
                            {not_found, State#state{last_sent = maps:put(ForActorID, {CF2, LastKey}, State#state.last_sent)}};
                        {Key, CF2, Msg, Multicast} ->
                            {Seq, State2} = make_seq(ForActorID, State),
                            {ok, Seq, Msg,
                             State2#state{pending_acks = maps:put(ForActorID, [{Seq, CF2, Key, Multicast}], Pending)}};
                        not_found ->
                            {not_found, State#state{last_sent = maps:put(ForActorID, none, State#state.last_sent)}}
                    end
            end
    end.

-spec reset_actor(pos_integer(), relcast_state()) -> {ok, relcast_state()}.
reset_actor(ForActorID, State = #state{pending_acks = Pending, last_sent = LastSent}) ->
    {ok, reset_seq(ForActorID, State#state{pending_acks = Pending#{ForActorID => []},
                                           last_sent = maps:remove(ForActorID, LastSent)})}.

-spec in_flight(pos_integer(), relcast_state()) -> non_neg_integer().
in_flight(ForActorID, State = #state{pending_acks = Pending}) ->
    P = maps:get(ForActorID, Pending, []),
    Active = lists:filter(fun ({_Ref, CF, _Key, _Multicast}) ->
                                  CF == State#state.active_cf orelse CF == State#state.prev_cf
                          end, P),
    length(Active).

%%% @doc Get the next message this relcast has queued outbound for
%%% `ForActorID', without affecting the pipeline state or having any other side effects.
-spec peek(pos_integer(), relcast_state()) ->
                  not_found |
                  {ok, binary()}.
peek(ForActorID, State = #state{pending_acks = Pending}) ->
    %% we need to find the first "unacked" message for this actor
    %% we should remember the last acked message for this actor ID and start there
    %% check if there's a pending ACK and use that to find the "last" key, if present
    case maps:get(ForActorID, Pending, []) of
        Pends when Pends /= [] ->
            case lists:last(Pends) of
                {_Ref, CF, Key, _Multicast} when CF == State#state.active_cf; CF == State#state.prev_cf ->
                    %% iterate until we find a key for this actor
                    case find_next_outbound(ForActorID, CF, Key, State, false) of
                        {not_found, _LastKey, _CF2} ->
                            not_found;
                        {_Key2, _CF2, Msg, _Multicast2} ->
                            {ok, Msg};
                        not_found ->
                            not_found
                    end;
                _ ->
                    %% here when the CF is old, we need to re-search in a newer
                    %% epoch.  our state alteration will be undone, since we
                    %% never pass the changed state back to the user
                    peek(ForActorID, State#state{pending_acks = Pending#{ForActorID => []}})
            end;
        _ ->
            %% default to the "first" key"
            case maps:get(ForActorID, State#state.last_sent, {prev_cf(State), min_outbound_key()}) of
                none ->
                    %% we *know* there's nothing pending for this actor
                    not_found;
                {CF0, StartKey0} ->
                    %% check if the column family is still valid
                    {CF, StartKey} = case CF0 == State#state.active_cf orelse CF0 == State#state.prev_cf of
                                         true ->
                                             {CF0, StartKey0};
                                         false ->
                                             %% reset the start key as well
                                             {State#state.prev_cf, min_outbound_key()}
                                     end,
                    %% iterate until we find a key for this actor
                    case find_next_outbound(ForActorID, CF, StartKey, State) of
                        {not_found, _LastKey, _CF2} ->
                            not_found;
                        {_Key, _CF2, Msg, _Multicast} ->
                            {ok, Msg};
                        not_found ->
                            not_found
                    end
            end
    end.

ack(FromActorID, Seq, State) ->
    ack(FromActorID, Seq, false, State).

multi_ack(FromActorID, Seq, State) ->
    ack(FromActorID, Seq, true, State).

%% @doc Indicate to relcast that `FromActorID' has acknowledged receipt of the
%% message associated with `Seq'.
-spec ack(pos_integer(), non_neg_integer(), boolean(), relcast_state()) -> {ok, relcast_state()}.
ack(FromActorID, Seq, MultiAck, State = #state{db = DB}) ->
    case maps:get(FromActorID, State#state.pending_acks, []) of
        [] ->
            {ok, State};
        Pends ->
            case lists:keyfind(Seq, 1, Pends) of
                {Seq, CF, AKey, Multicast} when CF == State#state.active_cf;
                                                 CF == State#state.prev_cf ->
                    NewPends = case MultiAck of
                                   true ->
                                       %% in the case that we get an ack that is not the first, we
                                       %% ack everything up to the acked message.  keyfind is fast
                                       %% but sadly doesn't return the index, so we get this fold:
                                       {_, NewPends0} =
                                       lists:foldl(
                                         %% this clause iterates through the list until it
                                         %% finds the entry with Seq, deleting as it goes.
                                         fun({R, Fam, Key, MCast}, false) ->
                                                 case MCast of
                                                     false ->
                                                         %% unicast message, fine to delete now
                                                         rocksdb:delete(DB, Fam, Key, []);
                                                     true ->
                                                         %% flip the bit, we can delete it next time we iterate
                                                         flip_actor_bit(FromActorID, DB, Fam, Key)
                                                 end,
                                                 case R of
                                                     Seq ->
                                                         %% when we find it we switch over to
                                                         %% clause #2
                                                         {true, []};
                                                     _ ->
                                                         false
                                                 end;
                                            %% once we're here just accumulate the leftovers
                                            (Elt, {true, Acc}) ->
                                                 {true, [Elt | Acc]}
                                         end,
                                         false,
                                         Pends),
                                       %% then reverse them since we've changed the order.
                                       lists:reverse(NewPends0);
                                   false ->
                                       case Multicast of
                                           false ->
                                               %% unicast message, fine to delete now
                                               rocksdb:delete(DB, CF, AKey, []);
                                           true ->
                                               %% flip the bit, we can delete it next time we iterate
                                               flip_actor_bit(FromActorID, DB, CF, AKey)
                                       end,
                                       lists:keydelete(Seq, 1, Pends)
                               end,
                    NewPending = (State#state.pending_acks)#{FromActorID => NewPends},
                    {ok, State#state{pending_acks=NewPending, last_sent = maps:put(FromActorID, {CF, AKey}, State#state.last_sent)}};
                _ ->
                    %% delete this, it's stale
                    NewPends = lists:keydelete(Seq, 1, Pends),
                    NewPending = (State#state.pending_acks)#{FromActorID => NewPends},
                    {ok, State#state{pending_acks=NewPending}}
            end
    end.

%% @doc Stop the relcast instance.
-spec stop(any(), relcast_state()) -> ok.
stop(Reason, State = #state{module=Module, modulestate=ModuleState})->
    case erlang:function_exported(Module, terminate, 2) of
        true ->
            Module:terminate(Reason, ModuleState);
        false ->
            ok
    end,
    rocksdb:close(State#state.db).

%% @doc Get a representation of the relcast's module state, inbound queue and
%% outbound queue.
-spec status(relcast_state()) -> status().
status(State = #state{modulestate=ModuleState}) ->
    {ok, Iter} = cf_iterator(State, prev_cf(State), outbound, [{iterate_upper_bound, max_outbound_key()}]),
    OutboundQueue = build_outbound_status(cf_iterator_move(Iter, {seek, min_outbound_key()}), Iter, State#state.bitfieldsize, #{}),
    {ok, InIter} = rocksdb:iterator(State#state.db, State#state.inbound_cf, [{iterate_upper_bound, max_inbound_key()}]),
    InboundQueue = build_inbound_status(rocksdb:iterator_move(InIter, {seek, min_inbound_key()}), InIter, []),
    {ModuleState, InboundQueue, OutboundQueue}.

%%====================================================================
%% Internal functions
%%====================================================================

-spec handle_pending_inbound(rocksdb:batch_handle(), relcast_state()) -> {stop, pos_integer(), relcast_state()} | {ok, relcast_state()}.
handle_pending_inbound(Batch, State) ->
    %% so we need to start at the oldest messages in the inbound queue and
    %% attempt Module:handle_message on each one. If the module returns `defer'
    %% we need to not attempt to deliver any newer messages from that actor.
    %% This function returns when either all actors have hit a defer, or we run
    %% out of messages to examine. If we are successful in handling any inbound
    %% messages during the run, we should loop back to the oldest messages and
    %% try to handle them again, as the module may now be ready to handle them.
    {ok, Iter} = rocksdb:iterator(State#state.db, State#state.inbound_cf, [{iterate_upper_bound, max_inbound_key()}]),
    Res = rocksdb:iterator_move(Iter, {seek, min_inbound_key()}),
    case find_next_inbound(Res, Iter, Batch, false, [], State) of
        {stop, Timeout, State} ->
            {stop, Timeout, State};
        {ok, false, _, State} ->
            %% nothing changed, we're done here
            {ok, State};
        {ok, true, Acc, NewState} ->
            %% we changed something, try handling other deferreds again
            %% we have them in an accumulator, so we can just try to handle/delete them
            handle_defers(Batch, Acc, [], false, NewState)
    end.

find_next_inbound({error, _}, Iter, _Batch, Changed, Acc, State) ->
    ok = rocksdb:iterator_close(Iter),
    {ok, Changed, lists:reverse(Acc), State};
find_next_inbound({ok, <<"i", _/binary>> = Key, <<FromActorID:16/integer, Msg/binary>>}, Iter, Batch, Changed, Acc, State) ->
    CF = State#state.inbound_cf,
    case handle_message(Key, CF, FromActorID, Msg, Batch, State) of
        defer ->
            %% keep on going
            find_next_inbound(rocksdb:iterator_move(Iter, next), Iter, Batch, Changed, [{CF, Key, FromActorID, Msg}|Acc], State);
        ignore ->
            %% keep on going
            find_next_inbound(rocksdb:iterator_move(Iter, next), Iter, Batch, Changed, Acc, State);
        {ok, NewState} ->
            %% we managed to handle a deferred message, yay
            OldDefers = maps:get(FromActorID, NewState#state.defers),
            find_next_inbound(rocksdb:iterator_move(Iter, next), Iter, Batch, true, Acc,
                              NewState#state{defers=maps:put(FromActorID, OldDefers -- [Key], NewState#state.defers)});
        {stop, Timeout, NewState} ->
            ok = rocksdb:iterator_close(Iter),
            {stop, Timeout, NewState}
    end.

handle_defers(Batch, [], Out, true, State) ->
    %% we changed something, go around again
    handle_defers(Batch, Out, [], false, State);
handle_defers(_Batch, [], _Out, false, State) ->
    %% no changes this iteration, bail
    {ok, State};
handle_defers(Batch, [{CF, Key, FromActorID, Msg}|Acc], Out, Changed, State) ->
    case handle_message(Key, CF, FromActorID, Msg, Batch, State) of
        defer ->
            handle_defers(Batch, Acc, [{CF, Key, FromActorID, Msg}|Out], Changed, State);
        ignore ->
            handle_defers(Batch, Acc, [{CF, Key, FromActorID, Msg}|Out], Changed, State);
        {ok, NewState} ->
            OldDefers = maps:get(FromActorID, NewState#state.defers),
            handle_defers(Batch, Acc, Out, true,
                          NewState#state{defers=maps:put(FromActorID, OldDefers -- [Key], NewState#state.defers)});
        {stop, Timeout, NewState} ->
            {stop, Timeout, NewState}
    end.


handle_message(Key, CF, FromActorID, Message, Batch, State = #state{module=Module, modulestate=ModuleState}) ->
    case Module:handle_message(Message, FromActorID, ModuleState) of
        ignore ->
            case Key /= undefined of
                true ->
                    ok = rocksdb:batch_delete(Batch, CF, Key);
                false ->
                    ok
            end,
            ignore;
        defer ->
            defer;
        {NewModuleState, Actions} ->
            %% write new outbound messages, update the state and (if present) delete the message atomically
            case Key /= undefined of
                true ->
                    ok = rocksdb:batch_delete(Batch, CF, Key);
                false ->
                    ok
            end,
            case handle_actions(Actions, Batch, State#state{modulestate=NewModuleState}) of
                {ok, NewState} ->
                    ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                    {ok, NewState};
                {stop, Timeout, NewState} ->
                    ok = rocksdb:batch_put(Batch, NewState#state.active_cf, <<"stored_module_state">>, Module:serialize(NewState#state.modulestate)),
                    {stop, Timeout, NewState}
            end
    end.

%% write all resulting messages and keys in an atomic batch
handle_actions([], _Batch, State) ->
    {ok, State};
handle_actions([new_epoch|Tail], Batch, State) ->
    {ok, NewCF} = rocksdb:create_column_family(State#state.db, make_column_family_name(State#state.epoch + 1), db_options(length(State#state.ids))),
    Pends =
        case State#state.prev_cf of
            undefined -> State#state.pending_acks;
            _ ->
                ok = rocksdb:drop_column_family(State#state.prev_cf),
                ok = rocksdb:destroy_column_family(State#state.prev_cf),
                maps:map(fun(_, V) ->
                                 lists:filter(fun({_, CF, _, _}) -> CF /= State#state.prev_cf end, V)
                         end, State#state.pending_acks)
        end,
    %% when we're done handling actions, we will write the module state (and all subsequent outbound
    %% messages from this point on) into the active CF, which is this new one now
    handle_actions(Tail, Batch, State#state{out_key_count=0, active_cf=NewCF, prev_cf=State#state.active_cf,
                                            epoch=State#state.epoch + 1, pending_acks=Pends});
handle_actions([{multicast, Message}|Tail], Batch, State =
               #state{out_key_count=KeyCount, bitfieldsize=BitfieldSize, id=ID, ids=IDs, active_cf=CF, module=Module}) ->
    Bitfield = make_bitfield(BitfieldSize, IDs, ID),
    Key = make_outbound_key(KeyCount),
    ok = rocksdb:batch_put(Batch, CF, Key, <<0:2/integer, Bitfield:BitfieldSize/bits, Message/binary>>),
    %% handle our own copy of the message
    %% deferring your own message is an error
    case Module:handle_message(Message, ID, State#state.modulestate) of
        ignore ->
            handle_actions(Tail, Batch, update_next(IDs -- [ID], CF, Key, State#state{out_key_count=KeyCount+1}));
        {ModuleState, Actions} ->
            handle_actions(Actions++Tail, Batch, update_next(IDs -- [ID], CF, Key, State#state{modulestate=ModuleState, out_key_count=KeyCount+1}))
    end;
handle_actions([{callback, Message}|Tail], Batch, State =
               #state{out_key_count=KeyCount, bitfieldsize=BitfieldSize, id=ID, ids=IDs, active_cf=CF, module=Module}) ->
    Bitfield = make_bitfield(BitfieldSize, IDs, ID),
    Key = make_outbound_key(KeyCount),
    ok = rocksdb:batch_put(Batch, CF, Key, <<2:2/integer, Bitfield:BitfieldSize/bits, Message/binary>>),
    case Module:callback_message(ID, Message, State#state.modulestate) of
        none ->
            handle_actions(Tail, Batch, update_next(IDs -- [ID], CF, Key, State#state{out_key_count=KeyCount+1}));
        OurMessage when is_binary(OurMessage) ->
            %% handle our own copy of the message
            %% deferring your own message is an error
            case Module:handle_message(OurMessage, ID, State#state.modulestate) of
                ignore ->
                    handle_actions(Tail, Batch, update_next(IDs -- [ID], CF, Key, State#state{out_key_count=KeyCount+1}));
                {ModuleState, Actions} ->
                    handle_actions(Actions++Tail, Batch, update_next(IDs -- [ID], CF, Key, State#state{modulestate=ModuleState, out_key_count=KeyCount+1}))
            end
    end;
handle_actions([{unicast, ID, Message}|Tail], Batch, State = #state{module=Module, id=ID}) ->
    %% handle our own message
    %% deferring your own message is an error
    case Module:handle_message(Message, ID, State#state.modulestate) of
        ignore ->
            handle_actions(Tail, Batch, State);
        {ModuleState, Actions} ->
            handle_actions(Actions++Tail, Batch, State#state{modulestate=ModuleState})
    end;
handle_actions([{unicast, ToActorID, Message}|Tail], Batch, State = #state{out_key_count=KeyCount, active_cf=CF}) ->
    Key = make_outbound_key(KeyCount),
    ok = rocksdb:batch_put(Batch, CF, Key, <<1:2/integer, ToActorID:14/integer, Message/binary>>),
    handle_actions(Tail, Batch, update_next([ToActorID], CF, Key, State#state{out_key_count=KeyCount+1}));
handle_actions([{stop, Timeout}|_Tail], _Batch, State) ->
    {stop, Timeout, State}.

update_next(Actors, CF, Key, State) ->
    LastSent = maps:map(fun(K, none) ->
                                case lists:member(K, Actors) of
                                    true ->
                                        {CF, Key};
                                    false ->
                                        none
                                end;
                           (_, V) -> V
                        end, State#state.last_sent),
    State#state{last_sent=LastSent}.

make_bitfield(BitfieldSize, Actors, Actor) ->
    Bits = << begin
                  case A of
                      Actor ->
                          <<0:1/integer>>;
                      _ ->
                          <<1:1/integer>>
                  end
              end || A <- Actors >>,
    <<Bits:(length(Actors))/bits, 0:(BitfieldSize - length(Actors))/integer>>.

db_options(NumActors) ->
    [
     {create_if_missing, true},
     {max_open_files, 1024},
     {max_log_file_size, 100*1024*1024},
     {merge_operator, {bitset_merge_operator, round_to_nearest_byte(NumActors+2)}}
    ].

round_to_nearest_byte(Bits) ->
    case Bits rem 8 of
        0 ->
            Bits;
        Extra ->
            Bits + (8 - Extra)
    end.

%% get the maximum key ID used
get_last_key_in(DB, CF) ->
    {ok, InIter} = rocksdb:iterator(DB, CF, [{iterate_lower_bound, min_inbound_key()}]),
    %% XXX iterate_upper_bound doesn't work, so we can't use it.
    %% instead we seek to the last possible key, and if that is not present,
    %% seek to the previous key
    MaxInbound = case rocksdb:iterator_move(InIter, last) of
        {ok, <<"i", InNum:10/binary>>, _} ->
        list_to_integer(binary_to_list(InNum));
        _E1 ->
            case rocksdb:iterator_move(InIter, prev) of
                {ok, <<"i", InNum:10/binary>>, _} ->
                    list_to_integer(binary_to_list(InNum));
                _E2 ->
                    0
            end
    end,
    rocksdb:iterator_close(InIter),
    MaxInbound.

get_last_key_out(DB, CF) ->
    {ok, OutIter} = rocksdb:iterator(DB, CF, [{iterate_lower_bound, min_outbound_key()}]),
    MaxOutbound = case rocksdb:iterator_move(OutIter, last) of
        {ok, <<"o", OutNum:10/binary>>, _} ->
            list_to_integer(binary_to_list(OutNum));
        _E1 ->
            case rocksdb:iterator_move(OutIter, prev) of
                {ok, <<"o", OutNum:10/binary>>, _} ->
                    list_to_integer(binary_to_list(OutNum));
                _E2 ->
                    0
            end
    end,
    rocksdb:iterator_close(OutIter),
    MaxOutbound.

%% iterate the outbound messages until we find one for this ActorID
find_next_outbound(ActorID, CF, StartKey, State) ->
    find_next_outbound(ActorID, CF, StartKey, State, true).

find_next_outbound(ActorID, CF, StartKey, State, AcceptStart) ->
    {ok, Iter} = cf_iterator(State, CF, outbound, [{iterate_upper_bound, max_outbound_key()}]),
    Res =
        case AcceptStart of
            true ->
                cf_iterator_move(Iter, StartKey);
            false ->
                %% on the paths where this is called, we're calling this with a
                %% known to be existing start key, so we need to move the
                %% iterator past it initially so we don't get it back
                cf_iterator_move(Iter, StartKey),
                cf_iterator_move(Iter, next)
        end,
    find_next_outbound_(ActorID, Res, Iter, State).

find_next_outbound_(_ActorId, {error, _}, Iter, _State) ->
    %% try to return the *highest* key we saw, so we can try starting here next time
    Res = case cf_iterator_move(Iter, prev) of
        {ok, Key, _} ->
            {not_found, Key, cf_iterator_id(Iter)};
        _ ->
            not_found
    end,
    cf_iterator_close(Iter),
    Res;
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<1:2/integer, ActorID:14/integer, Value/binary>>}, Iter, _State) ->
    %% unicast message for this actor
    CF = cf_iterator_id(Iter),
    cf_iterator_close(Iter),
    {Key, CF, Value, false};
find_next_outbound_(ActorID, {ok, <<"o", _/binary>>, <<1:2/integer, _/bits>>}, Iter, State) ->
    %% unicast message for someone else
    find_next_outbound_(ActorID, cf_iterator_move(Iter, next), Iter, State);
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<Type:2/integer, Tail/bits>>}, Iter, State = #state{bitfieldsize=BitfieldSize}) when Type == 0; Type == 2 ->
    <<ActorMask:BitfieldSize/integer-unsigned-big, Value/binary>> = Tail,
    case ActorMask band (1 bsl (BitfieldSize - ActorID)) of
        0 ->
            %% not for us, keep looking
            case ActorMask == 0 of
                true ->
                    %% everyone has gotten this message, we can delete it now
                    ok = rocksdb:delete(State#state.db, cf_iterator_id(Iter), Key, []);
                false ->
                    ok
            end,
            find_next_outbound_(ActorID, cf_iterator_move(Iter, next), Iter, State);
        _  when Type == 0 ->
            %% multicast message with the high bit set for this actor
            CF = cf_iterator_id(Iter),
            cf_iterator_close(Iter),
            {Key, CF, Value, true};
        _  when Type == 2 ->
            %% callback message with the high bit set for this actor
            CF = cf_iterator_id(Iter),
            Module = State#state.module,
            case Module:callback_message(ActorID, Value, State#state.modulestate) of
                none ->
                    %% nothing for this actor
                    flip_actor_bit(ActorID, State#state.db, CF, Key),
                    find_next_outbound_(ActorID, cf_iterator_move(Iter, next), Iter, State);
                Message ->
                    cf_iterator_close(Iter),
                    {Key, CF, Message, true}
            end

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

build_outbound_status({error, _}, Iter, _BFS, OutboundQueue) ->
    cf_iterator_close(Iter),
    maps:map(fun(_K, V) -> lists:reverse(V) end, OutboundQueue);
build_outbound_status({ok, <<"o", _/binary>>, <<1:2/integer, ActorID:14/integer, Value/binary>>}, Iter, BFS, OutboundQueue) ->
    %% unicast message
    build_outbound_status(cf_iterator_move(Iter, next), Iter, BFS, prepend_message([ActorID], Value, OutboundQueue));
build_outbound_status({ok, <<"o", _/binary>>, <<0:2/integer, Tail/bits>>}, Iter, BFS, OutboundQueue) ->
    <<ActorMask:BFS/bits, Value/binary>> = Tail,
    ActorIDs = actor_list(ActorMask, 1, []),
    build_outbound_status(cf_iterator_move(Iter, next), Iter, BFS, prepend_message(ActorIDs, Value, OutboundQueue));
build_outbound_status({ok, _Key, _Value}, Iter, BFS,  OutboundQueue) ->
    build_outbound_status(cf_iterator_move(Iter, next), Iter, BFS, OutboundQueue).

build_inbound_status({error, _}, Iter, InboundQueue) ->
    rocksdb:iterator_close(Iter),
    lists:reverse(InboundQueue);
build_inbound_status({ok, <<"i", _/binary>>, <<FromActorID:16/integer, Msg/binary>>}, Iter, InboundQueue) ->
    build_inbound_status(rocksdb:iterator_move(Iter, next), Iter, [{FromActorID, Msg}|InboundQueue]);
build_inbound_status({ok, _Key, _Value}, Iter, InboundQueue) ->
    build_inbound_status(rocksdb:iterator_move(Iter, next), Iter, InboundQueue).

build_defer_list({error, _}, Iter, _CF, Acc) ->
    rocksdb:iterator_close(Iter),
    Acc;
build_defer_list({ok, <<"i", _/binary>>=Key, <<FromActorID:16/integer, _Msg/binary>>}, Iter, CF, Acc) ->
    DefersForThisActor = maps:get(FromActorID, Acc, []),
    build_defer_list(rocksdb:iterator_move(Iter, next), Iter, CF, maps:put(FromActorID, [Key|DefersForThisActor], Acc)).

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

flip_actor_bit(ActorID, DB, CF, Key) ->
    ActorIDStr = ["-", integer_to_list(ActorID+1)],
    %% this can crash for odd reasons, but we don't care
    catch rocksdb:merge(DB, CF, Key, list_to_binary(ActorIDStr), []).


%% wrapper around rocksdb iterators that will iterate across column families.
%% Requirements to use this:
%% * you only iterate forwards
%% * you only iterate inbound/outbound messages
%% * jumps across column families will start in the next
%%   column family at the first key inbound/outbound key
-spec cf_iterator(relcast_state(), rocksdb:cf_handle(), inbound | outbound | both, list()) -> {ok, reference()}.
cf_iterator(State, CF, MsgType, Args) when MsgType == outbound ->
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

-spec cf_iterator_move(reference(), next | prev | {seek, binary()} | binary()) -> {ok, Key::binary(), Value::binary()} | {ok, Key::binary()} | {error, invalid_iterator} | {error, iterator_closed}.
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

-spec cf_iterator_close(reference()) -> ok.
cf_iterator_close(Ref) ->
    Iterator = erlang:get(Ref),
    erlang:erase(Ref),
    rocksdb:iterator_close(Iterator#iterator.iterator).

%% generates a partitioned sequence number with the actor ID in the high bits
%% rollover happens naturally because once the sequence number uses more than 32-PrefixLen
%% bits the high bits get dropped
make_seq(ID, #state{seq_map=SeqMap, ids=A}=State) ->
    Seq = maps:get(ID, SeqMap, 0),
    %% calculate the number of bits needed to hold length(A)
    PrefixLen = ceil(math:log2(length(A))),
    <<TaggedSeq:32/integer-unsigned-big>> = <<ID:PrefixLen, Seq:(32-PrefixLen)>>,
    {TaggedSeq, State#state{seq_map=maps:put(ID, Seq+1, SeqMap)}}.

reset_seq(ID, #state{seq_map=SeqMap}=State) ->
    State#state{seq_map=maps:remove(ID, SeqMap)}.


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

seq_increment_test() ->
    State = #state{ids=[1, 2, 3, 4, 5]},
    {Seq1, State1} = make_seq(1, State),
    {Seq2, _State2} = make_seq(1, State1),
    %% sequence numbers for the same actor should increment
    ?assert(Seq1 < Seq2),
    ?assertEqual(1, Seq2 - Seq1).

seq_partition_test() ->
    State = #state{ids=[1, 2, 3, 4, 5]},
    {Seq1, State1} = make_seq(1, State),
    {Seq2, _State2} = make_seq(2, State1),
    %% Sequence 0 for 2 actors should not be the same
    ?assertNotEqual(Seq1, Seq2).

seq_rollover_test() ->
    State = #state{ids=[1, 2, 3, 4, 5], seq_map=#{1 => trunc(math:pow(2, 29)) - 1}},
    {Seq1, State1} = make_seq(1, State),
    {Seq2, _State2} = make_seq(1, State1),
    ?assert(Seq1 > Seq2).

-endif.
