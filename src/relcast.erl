
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
%%  the module can return the 'new_epoch' which deletes all queued outbound messages.
%%  Messages older than that are, by definition, not necessary for the protocol to
%%  continue advancing and can be discarded.
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
   {reply, Reply :: term(), Actions :: actions(), NewState :: term() | ignore} |
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

-record(state,
         {
          db :: rocksdb:db_handle(),
          module :: atom(),
          modulestate :: any(),
          old_serialized :: undefined | map() | binary(),
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
          active_cf :: rocksdb:cf_handle(),
          defers = #{} :: #{pos_integer() => [binary()]},
          seq_map = #{} :: #{pos_integer() => pos_integer()},
          transaction :: undefined | rocksdb:transaction_handle(),
          transaction_dirty = false :: boolean(),
          new_defers :: undefined | integer(),  % right now this is just a counter for delivers
          last_defer_check :: undefined | integer(),
          key_tree :: [any()],
          key_tree_checked = false :: boolean(),
          db_opts = [] :: [any()],
          write_opts = [] :: [any()]
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
         process_inbound/1,
         stop/2,
         status/1
        ]).

-define(stored_module_state, <<"stored_module_state">>).
-define(stored_key_prefix, <<"stored_key_">>).
-define(stored_key_tree, <<"stored_key_tree">>).

%% TODO: remove these when fix goes in
-dialyzer({nowarn_function, [transaction/2]}).

-spec transaction(_, _) -> {ok, rocksdb:transaction_handle()}.
transaction(A, B) ->
    {ok, Txn} = rocksdb:transaction(A, B),
    {ok, Txn}.

%% @doc Start a relcast instance. Starts a relcast instance for the actor
%% `ActorID' in the group of `ActorIDs' using the callback module `Module'
%% initialized with `Arguments'. `RelcastOptions' contains configuration options
%% around the relcast itself, for example the data directory.
-spec start(pos_integer(), [pos_integer(),...], atom(), list(), list()) ->
                   error | {ok, relcast_state()} | {stop, pos_integer(), relcast_state()}.
start(ActorID, ActorIDs, Module, Arguments, RelcastOptions) ->
    DataDir = proplists:get_value(data_dir, RelcastOptions),
    DBOptions0 = db_options(length(ActorIDs)),
    OpenOpts1 = application:get_env(relcast, db_open_opts, []),
    OpenOpts2 = proplists:get_value(db_opts, RelcastOptions, []),
    WriteOpts = proplists:get_value(write_opts, RelcastOptions, [{sync, true}]),
    OpenOpts = OpenOpts1 ++ OpenOpts2,
    DBOptions = DBOptions0 ++ OpenOpts,
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
    {ok, DB, [_DefaultCF|CFHs0]} =
        rocksdb:open_optimistic_transaction_db(DataDir,
                                               [{create_if_missing, true}] ++ OpenOpts,
                                               [ {CF, DBOptions}
                                                 || CF <- ["default"|ColumnFamilies] ]),
    {InboundCF, CFHs} = case HasInbound of
                            false ->
                                {ok, ICF} = rocksdb:create_column_family(DB, "Inbound", DBOptions),
                                {ICF, CFHs0};
                            true ->
                                {hd(CFHs0), tl(CFHs0)}
                        end,
    %% check if we have some to prune
    %% delete all but the two newest *contiguous* column families
    {Epoch, ActiveCF} = case lists:reverse(ColumnFamilies -- ["Inbound"]) of
                             [] ->
                                 %% no column families, create epoch 0
                                 {ok, FirstCF} = rocksdb:create_column_family(DB, make_column_family_name(0), DBOptions),
                                 {0, FirstCF};
                             [JustOne] ->
                                 %% only a single column family, no need to prune
                                 {cf_to_epoch(JustOne), hd(CFHs)};
                             [Last | _Tail] ->
                                %% Prune all but the latest epoch
                                CFsToDelete = lists:sublist(CFHs, 1, length(CFHs) - 1),
                                [ ok = rocksdb:drop_column_family(CFH) || CFH <- CFsToDelete ],
                                [ ok = rocksdb:destroy_column_family(CFH) || CFH <- CFsToDelete ],
                                {cf_to_epoch(Last), hd(lists:sublist(CFHs, length(CFHs) + 1 - 1, 1))}
                         end,
    case Module:init(Arguments) of
        {ok, ModuleState0} ->
            {OldSer, ModuleState, KeyTree} = get_mod_state(DB, InboundCF, Module, ModuleState0, WriteOpts),
            LastKeyIn = get_last_key_in(DB, InboundCF),
            LastKeyOut = get_last_key_out(DB, ActiveCF),
            BitFieldSize = round_to_nearest_byte(length(ActorIDs) + 2) - 2, %% two bits for unicast/multicast
            State = #state{module = Module,
                           id = ActorID,
                           inbound_cf = InboundCF,
                           active_cf = ActiveCF,
                           ids = ActorIDs,
                           modulestate = ModuleState,
                           old_serialized = OldSer,
                           db = DB,
                           out_key_count = LastKeyOut + 1,
                           in_key_count = LastKeyIn + 1,
                           epoch = Epoch,
                           bitfieldsize = BitFieldSize,
                           db_opts = DBOptions,
                           write_opts = WriteOpts,
                           key_tree = KeyTree},
            {ok, Iter} = rocksdb:iterator(State#state.db, InboundCF, [{iterate_upper_bound, max_inbound_key()}]),
            Defers = build_defer_list(rocksdb:iterator_move(Iter, {seek, min_inbound_key()}), Iter, InboundCF, #{}),
            %% try to deliver any old queued inbound messages
            {ok, Transaction} = transaction(DB, WriteOpts),
            {ok, NewState} = handle_pending_inbound(Transaction, State#state{defers=Defers}),
            ok = rocksdb:transaction_commit(Transaction),
            {ok, Transaction1} = transaction(DB, WriteOpts),
            {ok, NewState#state{transaction = Transaction1}};
        _ ->
            error
    end.

%% @doc Send a command to the relcast callback module. Commands are distinct
%% from messages as they do not originate from another actor in the relcast
%% group. Commands are dispatched to `Module':handle_command and can simply
%% return information via `{reply, Reply, ignore}' or update the callback
%% module's state or send messages via `{reply, Reply, Actions, NewModuleState}'.
-spec command(any(), relcast_state()) -> {any(), relcast_state()} | {stop, any(), pos_integer(), relcast_state()}.
command(Message, State = #state{module = Module,
                                modulestate = ModuleState,
                                transaction = Transaction}) ->
    case Module:handle_command(Message, ModuleState) of
        {reply, Reply, ignore} ->
            %% just returning information
            {Reply, State};
        {reply, Reply, Actions, NewModuleState} ->
            State1 = maybe_update_state(State, NewModuleState),
            %% write new output messages & update the state atomically
            case handle_actions(Actions, Transaction, State1) of
                {ok, NewState} ->
                    NewState1 = maybe_serialize(Module, ModuleState, NewState),
                    case handle_pending_inbound(NewState#state.transaction, NewState1) of
                        {ok, NewerState} ->
                            {Reply, NewerState};
                        {stop, Timeout, NewerState} ->
                            {stop, Reply, Timeout, NewerState}
                    end;
                {stop, Timeout, NewState} ->
                    NewState1 = maybe_serialize(Module, ModuleState, NewState),
                    {stop, Reply, Timeout, NewState1}
            end
    end.

%% @doc Deliver a message from another actor to the relcast instance. `Message'
%% from `FromActorID' is submitted via `Module':handle_message. Depending on the
%% result of this, the message is either consumed immediately, deferred for
%% later, or this function returns `full' to indicate it cannot absorb any more
%% deferred messages from this Actor.
-spec deliver(binary(), pos_integer(), relcast_state()) -> {ok, relcast_state()} | {stop, pos_integer(), relcast_state()} | full.
deliver(Message, FromActorID, State = #state{in_key_count = KeyCount,
                                             defers = Defers}) ->
    case handle_message(undefined, undefined, FromActorID, Message, State#state.transaction, State) of
        {ok, NewState0} ->
            %% we only need to do this until we get control over what
            %% acks a client can issue, but need it now for safety
            NewState = maybe_commit(NewState0),
            %% something happened, evaluate if we can handle any other blocked messages
            case length(maps:keys(Defers)) of
                0 ->
                    %% no active defers, no queued inbound messages to evaluate
                    {ok, NewState};
                _ ->
                    case handle_pending_inbound(NewState#state.transaction,
                                                NewState#state{new_defers = State#state.new_defers + 1}) of
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
                    ok = rocksdb:transaction_put(State#state.transaction, State#state.inbound_cf, Key,
                                                 <<FromActorID:16/integer, Message/binary>>),
                    {ok, State#state{in_key_count = KeyCount + 1,
                                     transaction_dirty = true,
                                     defers = maps:put(FromActorID, [Key|N], Defers)}};
                _ ->
                    %% sorry buddy, no room on the couch
                    full
            end
    end.

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
                  {pipeline_full, relcast_state()} |
                  {ok, non_neg_integer(), binary(), relcast_state()}.
take(ForActorID, State, true) ->
    {ok, NewState} = reset_actor(ForActorID, State),
    take(ForActorID, NewState, false);
take(ForActorID, State = #state{pending_acks = Pending}, _) ->
    %% we need to find the first "unacked" message for this actor
    %% we should remember the last acked message for this actor ID and start there
    %% check if there's a pending ACK and use that to find the "last" key, if present
    PipelineDepth = application:get_env(relcast, pipeline_depth, 50),
    case maps:get(ForActorID, Pending, []) of
        Pends when length(Pends) >= PipelineDepth ->
            {pipeline_full, State};
        Pends when Pends /= [] ->
            case lists:last(Pends) of
                {_Seq, CF, Key, _Multicast} when CF == State#state.active_cf ->
                    %% iterate until we find a key for this actor
                    case find_next_outbound(ForActorID, CF, Key, State, false) of
                        {not_found, LastKey, CF2} ->
                            {not_found, State#state{last_sent = maps:put(ForActorID, {CF2, LastKey}, State#state.last_sent)}};
                        {Key2, CF2, Msg, Multicast} ->
                            {Seq2, State2} = make_seq(ForActorID, State),
                            Pends1 = Pends ++ [{Seq2, CF2, Key2, Multicast}],
                            State3 = maybe_commit(State2),
                            {ok, Seq2, Msg,
                             State3#state{pending_acks = maps:put(ForActorID, Pends1, Pending)}};
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
            case maps:get(ForActorID, State#state.last_sent, {State#state.active_cf, min_outbound_key()}) of
                none ->
                    %% we *know* there's nothing pending for this actor
                    {not_found, State};
                {CF0, StartKey0} ->
                    %% check if the column family is still valid
                    {CF, StartKey} = case CF0 == State#state.active_cf of
                                         true ->
                                             {CF0, StartKey0};
                                         false ->
                                             %% reset the start key as well
                                             {State#state.active_cf, min_outbound_key()}
                                     end,
                    %% iterate until we find a key for this actor
                    case find_next_outbound(ForActorID, CF, StartKey, State) of
                        {not_found, LastKey, CF2} ->
                            {not_found, State#state{last_sent = maps:put(ForActorID, {CF2, LastKey}, State#state.last_sent)}};
                        {Key, CF2, Msg, Multicast} ->
                            {Seq, State2} = make_seq(ForActorID, State),
                            State3 = maybe_commit(State2),
                            {ok, Seq, Msg,
                             State3#state{pending_acks = maps:put(ForActorID, [{Seq, CF2, Key, Multicast}], Pending)}};
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
                                  CF == State#state.active_cf
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
                {_Ref, CF, Key, _Multicast} when CF == State#state.active_cf ->
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
            case maps:get(ForActorID, State#state.last_sent, {State#state.active_cf, min_outbound_key()}) of
                none ->
                    %% we *know* there's nothing pending for this actor
                    not_found;
                {CF0, StartKey0} ->
                    %% check if the column family is still valid
                    {CF, StartKey} = case CF0 == State#state.active_cf of
                                         true ->
                                             {CF0, StartKey0};
                                         false ->
                                             %% reset the start key as well
                                             {State#state.active_cf, min_outbound_key()}
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
ack(FromActorID, Seq, MultiAck, State = #state{transaction = Transaction,
                                               bitfieldsize = BFS}) ->
    case maps:get(FromActorID, State#state.pending_acks, []) of
        [] ->
            {ok, State};
        Pends ->
            erlang:put(dirty, false),
            case lists:keyfind(Seq, 1, Pends) of
                {Seq, CF, AKey, Multicast} when CF == State#state.active_cf ->
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
                                                 erlang:put(dirty, true),
                                                 case MCast of
                                                     false ->
                                                         %% unicast message, fine to delete now
                                                         ok = rocksdb:transaction_delete(Transaction, Fam, Key);
                                                     true ->
                                                         %% flip the bit, we can delete it next time we iterate
                                                         flip_actor_bit(FromActorID, Transaction, Fam, Key, BFS)
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
                                       erlang:put(dirty, true),
                                       case Multicast of
                                           false ->
                                               %% unicast message, fine to delete now
                                               ok = rocksdb:transaction_delete(Transaction, CF, AKey);
                                           true ->
                                               %% flip the bit, we can delete it next time we iterate
                                               flip_actor_bit(FromActorID, Transaction, CF, AKey, BFS)
                                       end,
                                       lists:keydelete(Seq, 1, Pends)
                               end,
                    Dirty = erlang:get(dirty),
                    erlang:erase(dirty),
                    NewPending = (State#state.pending_acks)#{FromActorID => NewPends},
                    {ok, maybe_dirty(Dirty, State#state{pending_acks=NewPending,
                                                        last_sent = maps:put(FromActorID, {CF, AKey},
                                                                             State#state.last_sent)})};
                _ ->
                    %% delete this, it's stale
                    NewPends = lists:keydelete(Seq, 1, Pends),
                    NewPending = (State#state.pending_acks)#{FromActorID => NewPends},
                    {ok, State#state{pending_acks=NewPending}}
            end
    end.

%% @doc Allow inbound processing to be externally triggered so that we
%% don't get "stuck" with delayed defers blocking the forward progress
%% of the state machine defined by the behavior.
-spec process_inbound(relcast_state()) ->
                             {ok, relcast_state()} |
                             {stop, pos_integer(), relcast_state()}.
process_inbound(State) ->
    case handle_pending_inbound(State#state.transaction, State) of
        {ok, NewState} ->
            {ok, NewState};
        {stop, Timeout, NewState} ->
            {stop, Timeout, NewState}
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
    catch rocksdb:transaction_commit(State#state.transaction),
    rocksdb:close(State#state.db).

%% @doc Get a representation of the relcast's module state, inbound queue and
%% outbound queue.
-spec status(relcast_state()) -> status().
status(State = #state{modulestate = ModuleState, transaction = Transaction}) ->
    {ok, Iter} = rocksdb:transaction_iterator(State#state.db, Transaction, State#state.active_cf,
                                              [{iterate_upper_bound, max_outbound_key()}]),
    OutboundQueue = build_outbound_status(rocksdb:iterator_move(Iter, {seek, min_outbound_key()}),
                                          Iter, State#state.bitfieldsize, #{}),
    {ok, InIter} = rocksdb:transaction_iterator(State#state.db, Transaction, State#state.inbound_cf,
                                                [{iterate_upper_bound, max_inbound_key()}]),
    InboundQueue = build_inbound_status(rocksdb:iterator_move(InIter, {seek, min_inbound_key()}), InIter, []),
    {ModuleState, InboundQueue, OutboundQueue}.

%%====================================================================
%% Internal functions
%%====================================================================

-spec handle_pending_inbound(rocksdb:transaction_handle(), relcast_state()) ->
                                    {stop, pos_integer(), relcast_state()} | {ok, relcast_state()}.
handle_pending_inbound(Transaction, #state{new_defers = Defers,
                                           last_defer_check = Last0} = State) ->
    CountThreshold = application:get_env(relcast, defer_count_threshold, 20),
    TimeThreshold =  application:get_env(relcast, defer_time_threshold, 5000),
    Last = case Last0 of
               undefined -> 0;
               _ -> Last0
           end,
    Time = erlang:monotonic_time(milli_seconds) - Last,
    case (Defers == undefined orelse Defers > CountThreshold) orelse
        (Last0 == undefined orelse Time > TimeThreshold) of
        false ->
            {ok, State};
        true ->
            handle_pending_inbound_(Transaction, State)
    end.

handle_pending_inbound_(Transaction, State) ->
    %% so we need to start at the oldest messages in the inbound queue and
    %% attempt Module:handle_message on each one. If the module returns `defer'
    %% we need to not attempt to deliver any newer messages from that actor.
    %% This function returns when either all actors have hit a defer, or we run
    %% out of messages to examine. If we are successful in handling any inbound
    %% messages during the run, we should loop back to the oldest messages and
    %% try to handle them again, as the module may now be ready to handle them.
    {ok, Iter} = rocksdb:transaction_iterator(State#state.db, Transaction, State#state.inbound_cf,
                                              [{iterate_upper_bound, max_inbound_key()}]),
    Res = rocksdb:iterator_move(Iter, first),
    case find_next_inbound(Res, Iter, Transaction, false, [], State) of
        {stop, Timeout, State} ->
            {stop, Timeout, mark_defers(State)};
        {ok, false, _, State} ->
            %% nothing changed, we're done here
            {ok, mark_defers(State)};
        {ok, true, Acc, NewState} ->
            %% we changed something, try handling other deferreds again
            %% we have them in an accumulator, so we can just try to handle/delete them
            handle_defers(Transaction, Acc, [], false, mark_defers(NewState))
    end.

find_next_inbound({error, _}, Iter, _Transaction, Changed, Acc, State) ->
    ok = rocksdb:iterator_close(Iter),
    {ok, Changed, lists:reverse(Acc), State};
find_next_inbound({ok, <<"i", _/binary>> = Key, <<FromActorID:16/integer, Msg/binary>>}, Iter, Transaction, Changed, Acc, State) ->
    CF = State#state.inbound_cf,
    case handle_message(Key, CF, FromActorID, Msg, Transaction, State) of
        defer ->
            %% keep on going
            find_next_inbound(rocksdb:iterator_move(Iter, next), Iter, Transaction, Changed, [{CF, Key, FromActorID, Msg}|Acc], State);
        ignore ->
            %% keep on going
            find_next_inbound(rocksdb:iterator_move(Iter, next), Iter, Transaction, Changed, Acc, State);
        {ok, NewState} ->
            %% we managed to handle a deferred message, yay
            OldDefers = maps:get(FromActorID, NewState#state.defers),
            find_next_inbound(rocksdb:iterator_move(Iter, next), Iter, Transaction, true, Acc,
                              NewState#state{defers=maps:put(FromActorID, OldDefers -- [Key], NewState#state.defers)});
        {stop, Timeout, NewState} ->
            ok = rocksdb:iterator_close(Iter),
            {stop, Timeout, NewState}
    end.

handle_defers(Transaction, [], Out, true, State) ->
    %% we changed something, go around again
    handle_defers(Transaction, Out, [], false, State);
handle_defers(_Transaction, [], _Out, false, State) ->
    %% no changes this iteration, bail
    {ok, State};
handle_defers(Transaction, [{CF, Key, FromActorID, Msg}|Acc], Out, Changed, State) ->
    case handle_message(Key, CF, FromActorID, Msg, Transaction, State) of
        defer ->
            handle_defers(Transaction, Acc, [{CF, Key, FromActorID, Msg}|Out], Changed, State);
        ignore ->
            handle_defers(Transaction, Acc, [{CF, Key, FromActorID, Msg}|Out], Changed, State);
        {ok, NewState} ->
            OldDefers = maps:get(FromActorID, NewState#state.defers),
            handle_defers(Transaction, Acc, Out, true,
                          NewState#state{defers=maps:put(FromActorID, OldDefers -- [Key], NewState#state.defers)});
        {stop, Timeout, NewState} ->
            {stop, Timeout, NewState}
    end.


handle_message(Key, CF, FromActorID, Message, Transaction, State = #state{module=Module, modulestate=ModuleState}) ->
    case Module:handle_message(Message, FromActorID, ModuleState) of
        ignore ->
            State1 =
                case Key /= undefined of
                    true ->
                        ok = rocksdb:transaction_delete(Transaction, CF, Key),
                        {ok, State#state{transaction_dirty = true}};
                    false ->
                        ignore
                end,
            State1;
        defer ->
            defer;
        {NewModuleState, Actions} ->
            %% write new outbound messages, update the state and (if present) delete the message atomically
            Dirty =
                case Key /= undefined of
                    true ->
                        ok = rocksdb:transaction_delete(Transaction, CF, Key),
                        true;
                    false ->
                        false
                end,
            case handle_actions(Actions, Transaction, State#state{modulestate=NewModuleState}) of
                {ok, NewState} ->
                    NewState1 = maybe_serialize(Module, ModuleState, NewState),
                    {ok, maybe_dirty(Dirty, NewState1)};
                {stop, Timeout, NewState} ->
                    NewState1 = maybe_serialize(Module, ModuleState, NewState),
                    {stop, Timeout, maybe_dirty(Dirty, NewState1)}
            end
    end.

%% write all resulting messages and keys in an atomic transaction
handle_actions([], _Transaction, State) ->
    {ok, State};
handle_actions([new_epoch|Tail], Transaction, State) ->
    ok = rocksdb:transaction_commit(Transaction),
    {ok, NewCF} = rocksdb:create_column_family(State#state.db, make_column_family_name(State#state.epoch + 1),
                                               State#state.db_opts),
    ok = rocksdb:drop_column_family(State#state.active_cf),
    ok = rocksdb:destroy_column_family(State#state.active_cf),
    %% when we're done handling actions, we will write the module state (and all subsequent outbound
    %% messages from this point on) into the active CF, which is this new one now
    {ok, Transaction1} = transaction(State#state.db, State#state.write_opts),
    handle_actions(Tail, Transaction1, State#state{out_key_count=0, active_cf=NewCF,
                                                   transaction = Transaction1,
                                                   transaction_dirty = false,
                                                   epoch=State#state.epoch + 1, pending_acks=#{}});
handle_actions([{multicast, Message}|Tail], Transaction, State =
               #state{out_key_count=KeyCount, bitfieldsize=BitfieldSize, id=ID, ids=IDs, active_cf=CF, module=Module}) ->
    Bitfield = make_bitfield(BitfieldSize, IDs, ID),
    Key = make_outbound_key(KeyCount),
    ok = rocksdb:transaction_put(Transaction, CF, Key, <<0:2/integer, Bitfield:BitfieldSize/bits, Message/binary>>),
    %% handle our own copy of the message
    %% deferring your own message is an error
    case Module:handle_message(Message, ID, State#state.modulestate) of
        ignore ->
            handle_actions(Tail, Transaction, update_next(IDs -- [ID], CF, Key, State#state{out_key_count=KeyCount+1}));
        {ModuleState, Actions} ->
            handle_actions(Actions++Tail, Transaction, update_next(IDs -- [ID], CF, Key, State#state{modulestate=ModuleState, out_key_count=KeyCount+1}))
    end;
handle_actions([{callback, Message}|Tail], Transaction, State =
               #state{out_key_count=KeyCount, bitfieldsize=BitfieldSize, id=ID, ids=IDs, active_cf=CF, module=Module}) ->
    Bitfield = make_bitfield(BitfieldSize, IDs, ID),
    Key = make_outbound_key(KeyCount),
    ok = rocksdb:transaction_put(Transaction, CF, Key, <<2:2/integer, Bitfield:BitfieldSize/bits, Message/binary>>),
    case Module:callback_message(ID, Message, State#state.modulestate) of
        none ->
            handle_actions(Tail, Transaction, update_next(IDs -- [ID], CF, Key, State#state{out_key_count=KeyCount+1}));
        OurMessage when is_binary(OurMessage) ->
            %% handle our own copy of the message
            %% deferring your own message is an error
            case Module:handle_message(OurMessage, ID, State#state.modulestate) of
                ignore ->
                    handle_actions(Tail, Transaction, update_next(IDs -- [ID], CF, Key, State#state{out_key_count=KeyCount+1}));
                {ModuleState, Actions} ->
                    handle_actions(Actions++Tail, Transaction, update_next(IDs -- [ID], CF, Key, State#state{modulestate=ModuleState, out_key_count=KeyCount+1}))
            end
    end;
handle_actions([{unicast, ID, Message}|Tail], Transaction, State = #state{module=Module, id=ID}) ->
    %% handle our own message
    %% deferring your own message is an error
    case Module:handle_message(Message, ID, State#state.modulestate) of
        ignore ->
            handle_actions(Tail, Transaction, State);
        {ModuleState, Actions} ->
            handle_actions(Actions++Tail, Transaction, State#state{modulestate=ModuleState})
    end;
handle_actions([{unicast, ToActorID, Message}|Tail], Transaction, State = #state{out_key_count=KeyCount, active_cf=CF}) ->
    Key = make_outbound_key(KeyCount),
    ok = rocksdb:transaction_put(Transaction, CF, Key, <<1:2/integer, ToActorID:14/integer, Message/binary>>),
    handle_actions(Tail, Transaction, update_next([ToActorID], CF, Key, State#state{out_key_count=KeyCount+1}));
handle_actions([{stop, Timeout}|_Tail], _Transaction, State) ->
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

get_mod_state(DB, OldCF, Module, ModuleState0, WriteOpts) ->
    case rocksdb:get(DB, OldCF, ?stored_module_state, []) of
        {ok, SerializedModuleState} ->
            ok = rocksdb:put(DB, ?stored_module_state, SerializedModuleState, []),
            ok = rocksdb:delete(DB, OldCF, ?stored_module_state, []),
            rehydrate(Module, SerializedModuleState, ModuleState0);
        not_found ->
            case rocksdb:get(DB, ?stored_module_state, []) of
                {ok, SerializedModuleState} ->
                    {SerState, ModState, _} = rehydrate(Module, SerializedModuleState, ModuleState0),
                    {ok, Txn} = transaction(DB, WriteOpts),
                    New = Module:serialize(ModState),
                    KT =
                        case do_serialize(Module, undefined, New, ?stored_key_prefix, Txn) of
                            bin ->
                                bin;
                            KeyTree ->
                                ok = rocksdb:transaction_put(Txn, ?stored_key_tree,
                                                             term_to_binary(KeyTree, [compressed])),
                                ok = rocksdb:transaction_delete(Txn, ?stored_module_state),
                                KeyTree
                        end,
                    rocksdb:transaction_commit(Txn),
                    {SerState, ModState, KT};
                not_found ->
                    case rocksdb:get(DB, ?stored_key_tree, []) of
                        {ok, KeyTreeBin} ->
                            KeyTree = binary_to_term(KeyTreeBin),
                            {SerState, ModState, _KT} =
                                do_deserialize(Module, ModuleState0, ?stored_key_prefix, KeyTree, DB),
                            NewSer = Module:serialize(ModState),
                            case get_key_tree(Module, NewSer) of
                                KeyTree ->
                                    lager:info("state matches ~p", [KeyTree]),
                                    {SerState, ModState, KeyTree};
                                bin ->
                                    lager:info("state moved to bin ~p", [KeyTree]),
                                    ok = rocksdb:put(DB, ?stored_module_state, NewSer,
                                                     [{sync, true}]),
                                    ok = rocksdb:delete(DB, ?stored_key_tree, [{sync, true}]),
                                    {SerState, ModState, bin};
                                KeyTreeNew ->
                                    lager:info("state didn't match new ~p old ~p", [KeyTree, KeyTreeNew]),
                                    ok = rocksdb:put(DB, ?stored_key_tree,
                                                     term_to_binary(KeyTreeNew, [compressed]), [{sync, true}]),
                                    {SerState, ModState, KeyTree}
                            end;
                            %% try to force full keytree rewrite on startup
                        not_found ->
                            {undefined, ModuleState0, bin}
                    end
            end
    end.

rehydrate(Module, SerState, ModuleState0) ->
    OldModuleState = Module:deserialize(SerState),
    {ok, RestoredModuleState} = Module:restore(OldModuleState, ModuleState0),
    {SerState, RestoredModuleState, bin}.

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
    {ok, Iter} = rocksdb:transaction_iterator(State#state.db, State#state.transaction,
                                              CF, [{iterate_upper_bound, max_outbound_key()}]),
    Res =
        case AcceptStart of
            true ->
                rocksdb:iterator_move(Iter, StartKey);
            false ->
                %% on the paths where this is called, we're calling this with a
                %% known to be existing start key, so we need to move the
                %% iterator past it initially so we don't get it back
                rocksdb:iterator_move(Iter, StartKey),
                rocksdb:iterator_move(Iter, next)
        end,
    find_next_outbound_(ActorID, Res, Iter, State).

find_next_outbound_(_ActorId, {error, _}, Iter, State) ->
    %% try to return the *highest* key we saw, so we can try starting here next time
    Res = case rocksdb:iterator_move(Iter, prev) of
        {ok, Key, _} ->
            {not_found, Key, State#state.active_cf};
        _ ->
            not_found
    end,
    rocksdb:iterator_close(Iter),
    Res;
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<1:2/integer, ActorID:14/integer, Value/binary>>}, Iter, State) ->
    %% unicast message for this actor
    rocksdb:iterator_close(Iter),
    {Key, State#state.active_cf, Value, false};
find_next_outbound_(ActorID, {ok, <<"o", _/binary>>, <<1:2/integer, _/bits>>}, Iter, State) ->
    %% unicast message for someone else
    find_next_outbound_(ActorID, rocksdb:iterator_move(Iter, next), Iter, State);
find_next_outbound_(ActorID, {ok, <<"o", _/binary>> = Key, <<Type:2/integer, Tail/bits>>}, Iter,
                    State = #state{bitfieldsize=BitfieldSize}) when Type == 0; Type == 2 ->
    <<ActorMask:BitfieldSize/integer-unsigned-big, Value/binary>> = Tail,
    case ActorMask band (1 bsl (BitfieldSize - ActorID)) of
        0 ->
            %% not for us, keep looking
            case ActorMask == 0 of
                true ->
                    %% everyone has gotten this message, we can delete it now
                    ok = rocksdb:transaction_delete(State#state.transaction, State#state.active_cf, Key);
                false ->
                    ok
            end,
            find_next_outbound_(ActorID, rocksdb:iterator_move(Iter, next), Iter, maybe_dirty(ActorMask == 0, State));
        _  when Type == 0 ->
            %% multicast message with the high bit set for this actor
            rocksdb:iterator_close(Iter),
            {Key, State#state.active_cf, Value, true};
        _  when Type == 2 ->
            %% callback message with the high bit set for this actor
            Module = State#state.module,
            case Module:callback_message(ActorID, Value, State#state.modulestate) of
                none ->
                    %% nothing for this actor
                    flip_actor_bit(ActorID, State#state.transaction, State#state.active_cf, Key, BitfieldSize),
                    find_next_outbound_(ActorID, rocksdb:iterator_move(Iter, next), Iter, State);
                Message ->
                    rocksdb:iterator_close(Iter),
                    {Key, State#state.active_cf, Message, true}
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

build_outbound_status({error, _}, Iter, _BFS, OutboundQueue) ->
    rocksdb:iterator_close(Iter),
    maps:map(fun(_K, V) -> lists:reverse(V) end, OutboundQueue);
build_outbound_status({ok, <<"o", _/binary>>, <<1:2/integer, ActorID:14/integer, Value/binary>>}, Iter, BFS, OutboundQueue) ->
    %% unicast message
    build_outbound_status(rocksdb:iterator_move(Iter, next), Iter, BFS, prepend_message([ActorID], Value, OutboundQueue));
build_outbound_status({ok, <<"o", _/binary>>, <<0:2/integer, Tail/bits>>}, Iter, BFS, OutboundQueue) ->
    <<ActorMask:BFS/bits, Value/binary>> = Tail,
    ActorIDs = actor_list(ActorMask, 1, []),
    build_outbound_status(rocksdb:iterator_move(Iter, next), Iter, BFS, prepend_message(ActorIDs, Value, OutboundQueue));
build_outbound_status({ok, _Key, _Value}, Iter, BFS,  OutboundQueue) ->
    build_outbound_status(rocksdb:iterator_move(Iter, next), Iter, BFS, OutboundQueue).

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

flip_actor_bit(ActorID, Transaction, CF, Key, BFS) ->
    %% with transactions, we can't actually do a merge at this point,
    %% so we need to read, edit, and write the bitfield inside the transaction
    {ok, Bits} = rocksdb:transaction_get(Transaction, CF, Key),
    <<Type:2/bits, ActorMask:BFS/integer-unsigned-big, Post/bits>> = Bits,
    Mask2 = ActorMask band (bnot (1 bsl (BFS - ActorID))),
    ok = rocksdb:transaction_put(Transaction, CF, Key, <<Type/bits, Mask2:BFS/integer-unsigned-big, Post/bits>>).

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

maybe_serialize(_Mod, Old, #state{modulestate = New} = S) when Old == New ->
    S;
maybe_serialize(Mod, _Old, #state{modulestate = New0,
                                  old_serialized = Old,
                                  transaction = Transaction} = S) ->
    New = Mod:serialize(New0),
    _KeyTree = do_serialize(Mod, Old, New, ?stored_key_prefix, Transaction),
    S#state{old_serialized = New, transaction_dirty = true}.

old_size(M) when is_map(M) ->
    maps:size(M);
old_size(_) ->
    -1.

%% TODO: remove all keytree accumulation from here?
do_serialize(Mod, Old, New, Prefix, Transaction) ->
    case New of
        State when is_binary(State) ->
            ok = rocksdb:transaction_put(Transaction, ?stored_module_state, State),
            bin;
        StateMap ->
            S = lists:sort(maps:to_list(StateMap)),
            SSize = maps:size(StateMap),
            OSize = old_size(Old),
            O = case Old of
                    %% since we're serializing for the first time, we need to make sure that
                    %% everything gets written out, otherwise we have partial state that
                    %% won't restore correctly.
                    BorU when BorU == undefined orelse
                              is_binary(BorU) ->
                        lists:map(fun({K, _V}) -> {K, never_ever_match_with_anything} end, S);
                    _Size when OSize =/= SSize ->
                        SKeys = maps:keys(StateMap),
                        OKeys = maps:keys(Old),
                        Old1 = lists:foldl(fun(L, OM) ->
                                                   OM#{L => undefined}
                                           end,
                                           Old,
                                           SKeys -- OKeys),
                        maps:to_list(maps:without(OKeys -- SKeys, Old1));
                    _ ->
                        lists:sort(maps:to_list(Old))
                end,
            L = lists:zip(S, O),
            KeyTree =
                lists:map(
                  fun({{K, V}, {_, V}}) ->
                          %% should be a binary
                          K;
                     ({{K, V}, {_, OV}}) ->
                          KeyName = <<Prefix/binary, (atom_to_binary(K, utf8))/binary>>,
                          case is_map(V) of
                              true ->
                                  do_serialize(K, fixup_old_map(OV), V, <<KeyName/binary, "_">>, Transaction);
                              false ->
                                  ok = rocksdb:transaction_put(Transaction, KeyName, V),
                                  K
                          end
                  end,
                  L),
            [Mod | KeyTree]
    end.

get_key_tree(_, B) when is_binary(B) ->
    bin;
get_key_tree(Mod, Map) ->
    KT = lists:map(
           fun({K, V}) when is_map(V)->
                   get_key_tree(K, V);
              ({K, _V}) ->
                   K
           end,
           maps:to_list(Map)),
    [Mod | KT].

fixup_old_map(never_ever_match_with_anything) ->
    undefined;
fixup_old_map(M) ->
    M.

do_deserialize(Mod, NewState, Prefix, KeyTree, RocksDB) ->
    R = fun Rec(Pfix, [_Top | KT], DB) ->
                lists:foldl(
                  fun(K, Acc) when is_atom(K) ->
                          KeyName = <<Pfix/binary, (atom_to_binary(K, utf8))/binary>>,
                          Term = case rocksdb:get(DB, KeyName, []) of
                                     {ok, Bin} ->
                                         lager:info("~p -> ~p", [KeyName, Bin]),
                                         Bin;
                                     not_found ->
                                         lager:info("~p -> not_found", [KeyName]),
                                         undefined
                                 end,
                          Acc#{K => Term};
                     (L, Acc) when is_list(L) ->
                          K = hd(L),
                          KeyName = <<Pfix/binary, (atom_to_binary(K, utf8))/binary, "_">>,
                          Acc#{K => Rec(KeyName, L, DB)}
                  end,
                  #{},
                  KT);
            Rec(_, bin, DB) ->
                case rocksdb:get(DB, ?stored_module_state, []) of
                    {ok, Bin} ->
                        Bin;
                    not_found ->
                        not_found
                end
        end,
    Map = R(Prefix, KeyTree, RocksDB),
    {A, B, _} = rehydrate(Mod, Map, NewState),
    {A, B, KeyTree}.

maybe_update_state(State, ignore) ->
    State;
maybe_update_state(State, NewModuleState) ->
    State#state{modulestate=NewModuleState}.

maybe_commit(#state{transaction_dirty = false} = S) ->
    S;
maybe_commit(#state{transaction = Txn, db = DB, write_opts = Opts} = S) ->
    ok = rocksdb:transaction_commit(Txn),
    {ok, Txn1} = transaction(DB, Opts),
    S#state{transaction = Txn1, transaction_dirty = false}.

maybe_dirty(false, S) ->
    S;
maybe_dirty(_, S) ->
    S#state{transaction_dirty = true}.

mark_defers(S) ->
    S#state{new_defers = 0,
            last_defer_check = erlang:monotonic_time(milli_seconds)}.



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
