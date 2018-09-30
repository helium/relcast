-module(test_handler).

-behavior(relcast).

-export([
         init/1,
         handle_command/2,
         handle_message/3,
         serialize/1,
         deserialize/1,
         restore/2
        ]).

-record(state, {
          id :: pos_integer(),
          got_hello=false :: boolean(),
          got_self_hai=true :: boolean()
         }).

init(ID) ->
    {ok, #state{id=ID}}.

handle_command(is_done, State) ->
    {reply, State#state.got_hello andalso State#state.got_self_hai, [], State};
handle_command(Msg, State) ->
    io:format("handle_call, Msg: ~p", [Msg]),
    {reply, ok, [], State}.

handle_message(<<"hello">>, Actor, State) ->
    {State#state{got_hello=true}, [{unicast, Actor, <<"ehlo">>}, {multicast, <<"hai">>}]};
handle_message(<<"hai">>, Actor, State = #state{id=Actor}) ->
    {State#state{got_self_hai=true}, []};
handle_message(Msg, Actor, State) ->
    ct:pal("handle_message, Msg: ~p, Actor: ~p~n", [Msg, Actor]),
    {State, []}.

serialize(State) ->
    io:format("Serialize: ~p~n", [State]),
    term_to_binary(State).

deserialize(Binary) ->
    io:format("Deserialize: ~p~n", [Binary]),
    binary_to_term(Binary).

restore(OldState, NewState) ->
    io:format("OldState: ~p, NewState: ~p~n", [OldState, NewState]),
    {ok, NewState}.
