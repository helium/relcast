-module(test_handler).

-behavior(relcast).

-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_message/3,
         serialize/1,
         deserialize/1,
         restore/2
        ]).

-record(state, {
         }).

init(Args) ->
    io:format("Args: ~p~n", [Args]),
    {ok, #state{}}.

handle_call(Msg, From, State) ->
    io:format("handle_call, Msg: ~p, From: ~p", [Msg, From]),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    io:format("handle_cast, Msg: ~p~n", [Msg]),
    {noreply, State}.

handle_message(Msg, Actor, State) ->
    io:format("handle_message, Msg: ~p, Actor: ~p~n", [Msg, Actor]),
    {State, ok}.

serialize(State) ->
    io:format("Serialize: ~p~n", [State]),
    ok.

deserialize(Binary) ->
    io:format("Deserialize: ~p~n", [Binary]),
    ok.

restore(OldState, NewState) ->
    io:format("OldState: ~p, NewState: ~p~n", [OldState, NewState]),
    ok.
