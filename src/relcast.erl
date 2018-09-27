-module(relcast).

%% API exports
-export([]).

%%====================================================================
%% API functions
%%====================================================================
-callback init(Arguments :: any()) -> {ok, State :: term()}.
-callback restore(OldState :: term(), NewState :: term()) -> {ok, State :: term()}.
-callback serialize(State :: term()) -> Binary :: binary().
-callback deserialize(Binary :: binary()) -> State :: term().
-callback handle_message(Message :: binary(), ActorId :: pos_integer(), State :: term()) ->
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

-type actions() :: [ {send, multicast, Message :: binary()} |
                     {send, unicast, To :: pos_integer(), Message :: binary()} |
                     {stop, Timeout :: timeout()} | new_epoch ].

%%====================================================================
%% Internal functions
%%====================================================================
