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
          got_self_hai=true :: boolean(),
          seqmap = #{} :: map(),
          round = 0 :: non_neg_integer()
         }).

init(ID) ->
    {ok, #state{id=ID}}.

handle_command(round, State) ->
    {reply, State#state.round, [], State};
handle_command(next_round, State) ->
    {reply, ok, [], State#state{round=State#state.round+1}};
handle_command(seqmap, State) ->
    {reply, State#state.seqmap, [], State};
handle_command(is_done, State) ->
    {reply, State#state.got_hello andalso State#state.got_self_hai, [], State};
handle_command(Msg, State) ->
    io:format("handle_call, Msg: ~p", [Msg]),
    {reply, ok, [], State}.

handle_message(<<"seq", Int:8/integer>>, Actor, State = #state{seqmap=Seqmap}) ->
    case maps:get(Actor, Seqmap, 0) + 1 of
        Int when Int =< State#state.round ->
            {State#state{seqmap=maps:put(Actor, Int, Seqmap)}, []};
        Int ->
            %% valid, but not yet
            defer;
        _ ->
            %% invalid, drop it
            {State, []}
    end;
handle_message(<<"hello">>, Actor, State) ->
    {State#state{got_hello=true}, [{unicast, Actor, <<"ehlo">>}, {multicast, <<"hai">>}]};
handle_message(<<"hai">>, Actor, State = #state{id=Actor}) ->
    {State#state{got_self_hai=true}, []};
handle_message(Msg, Actor, State) ->
    ct:pal("handle_message, Msg: ~p, Actor: ~p~n", [Msg, Actor]),
    {State, []}.

serialize(State) ->
    ct:pal("Serialize: ~p~n", [State]),
    term_to_binary(State).

deserialize(Binary) ->
    ct:pal("Deserialize: ~p~n", [Binary]),
    binary_to_term(Binary).

restore(OldState, NewState) ->
    ct:pal("OldState: ~p, NewState: ~p~n", [OldState, NewState]),
    {ok, OldState}.
