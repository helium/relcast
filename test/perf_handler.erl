-module(perf_handler).

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

init([ID]) ->
    {ok, #{id => ID,
           store => <<>>,
           inc => 0}}.

handle_command(next_epoch, State) ->
    {reply, ok, [new_epoch], State};
handle_command({store, Bin}, State) ->
    {reply, ok, [], State#{store => Bin}};
handle_command(Msg, _State) ->
    ct:pal("catchall handle_command, Msg: ~p", [Msg]),
    {reply, ok, ignore}.

handle_message(<<"ping">>, Actor, State) ->
    Inc = maps:get(inc, State),
    {State#{inc => Inc + 1}, [{unicast, Actor, <<"pong">>}]};
handle_message(Msg, Actor, State) ->
    ct:pal("handle_message, Msg: ~p, Actor: ~p~n", [Msg, Actor]),
    {State, []}.

callback_message(_Actor, _, _State) ->
    <<>>.

serialize(State) ->
    maps:map(fun(_K, V) ->  erlang:term_to_binary(V) end, State).

deserialize(Map) ->
    maps:map(fun(_K, V) -> erlang:binary_to_term(V) end, Map).

restore(OldState, _NewState) ->
    {ok, OldState}.
