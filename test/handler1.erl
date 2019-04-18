-module(handler1).

-behavior(relcast).

-export([
         init/1,
         handle_command/2,
         handle_message/3,
         callback_message/3,
         serialize/1,
         deserialize/1,
         restore/2
        ]).

-record(state, {
                a :: atom(),
                b :: atom(),
                c :: atom(),
                d :: atom(),
                e :: atom()
               }).

init([]) ->
    {ok, #state{}}.

handle_command(populate, State) ->
    {reply, ok, [], State#state{a=a, b=b, c=c, d=d, e=e}};
handle_command(get, State) ->
    {reply, State, [], State};
handle_command(Msg, _State) ->
    io:format("handle_call, Msg: ~p", [Msg]),
    {reply, ok, ignore}.

handle_message(Msg, Actor, State) ->
    ct:pal("handle_message, Msg: ~p, Actor: ~p~n", [Msg, Actor]),
    {State, []}.

callback_message(_, _, _) ->
    ignore.

serialize(State) ->
    ct:pal("Serialize: ~p~n", [State]),
    term_to_binary(State).

deserialize(Binary) ->
    ct:pal("Deserialize: ~p~n", [Binary]),
    binary_to_term(Binary).

restore(OldState, _NewState) ->
    {ok, OldState}.
