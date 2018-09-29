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
          key_count = 0
         }).

-export([start/3, queue/3, take/2, ack/2]).

start(ActorID, ActorIDs, Module, Arguments) ->
    DataDir = proplists:get_value(data_dir, Arguments),
    DBOptions = db_options(),
    {ok, DB} = rocksdb:open(DataDir, DBOptions),
    case erlang:apply(Module, init, Arguments) of
        {ok, ModuleState0} ->
            ModuleState = case rocksdb:get(DB, <<"module_state">>) of
                              {ok, SerializedModuleState} ->
                                  {ok, OldModuleState} = Module:deserialize(SerializedModuleState),
                                  {ok, RestoredModuleState} = Module:restore(OldModuleState, ModuleState0),
                                  RestoredModuleState;
                              {error, not_found} ->
                                  ModuleState0
                          end,
            %% TODO try to deliver any old queued inbound messages
            {ok, #state{module=Module, id=ActorID, ids=ActorIDs, modulestate=ModuleState, db=DB}};
        _ ->
            error
    end.

deliver(Message, FromActorID, State = #state{module=Module, key_count=KeyCount, modulestate=ModuleState, db=DB}) ->
    Key = make_message_key(KeyCount), %% some kind of predictable, monotonic key
    rocksdb:store(DB, Key, <<FromActorID:16/integer, Message/binary>>),
    case Module:handle_message(Message, FromActorID, ModuleState) of
        defer ->
            %% leave the message queued but we can ACK it
            {ok, State};
        {NewModuleState, Actions} ->
            NewState = handle_actions(Actions, State),
            rocksdb:store(DB, <<"module_state">>, Module:serialize(NewModuleState)),
            {ok, NewState#state{modulestate=NewModuleState}}
    end.

take(ForActorID, State) ->
    %% we need to find the first "unacked" message for this actor
    %% we should remember the last acked message for this actor ID and start there
    %% check if there's a pending ACK and use that to find the "last" key, if present
    case maps:get(ForActorID, State#state.pending_acks, undefined) of
        {Ref, Key} ->
            {ok, Ref, rocksdb:get(DB, Key), State};
        _ ->
            Key = maps:get(ID, State#state.last_sent, <<0>>), %% default to the "first" key"
            %% iterate until we find a key where the bit for this actor is not cleared
            Iter = rocksdb:iterator(Key),
            {Key, Msg} = find_next_message_for(ForActorID, Iter),
            Ref = make_ref(),
            {ok, Ref, Msg, State#state{pending_acks=maps:put(ForActorID, {Ref, Key}, State#state.pending_acks)}}
    end.

ack(FromActorID, Ref, State) ->
    case maps:get(FromActorID, State#state.pending_acks, undefined) of
        {Ref, Key} ->
            NewPending = maps:delete(FromActorID, State#state.pending_acks),
            {ok, State#state{pending_acks=NewPending, last_sent=maps:put(FromActorID, Key, State#state.last_sent}};
        undefined ->
            {ok, State}
    end.


%%====================================================================
%% Internal functions
%%====================================================================

handle_actions([], State) ->
    State;
handle_actions([{multicast, Message}|Tail], State) ->
    Bitfield = make_bitfield(State#state.actor_ids),
    rocksdb:put(DB, make_outbound_key(), <<BitField/binary, Message/binary>>),
    handle_actions(Tail, State);
handle_actions([{unicast, ToActorID, Message}|Tail], State) ->
    Bitfield = make_bitfield(ToActorID),
    rocksdb:put(DB, make_outbound_key(), <<BitField/binary, Message/binary>>),
    handle_actions(Tail, State);
handle_actions([{stop, Timeout}|Tail], State) ->
    {stop, Timeout}.

db_options() ->
    [
     {create_if_missing, true},
     {max_open_files, 1024},
     {max_log_file_size, 100*1024*1024}
    ].
