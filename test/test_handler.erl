-module(test_handler).

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
          id :: pos_integer(),
          got_hello=false :: boolean(),
          got_self_hai=false :: boolean(),
          got_salutation=false :: boolean(),
          seqmap = #{} :: map(),
          round = 0 :: non_neg_integer()
         }).

init([ID]) ->
    {ok, #state{id=ID}}.

handle_command(next_epoch, State) ->
    {reply, ok, [new_epoch], State};
handle_command({init, To}, State) ->
    ct:pal("init ~p", [To]),
    {reply, ok, [{unicast, To, <<"hello">>}], State};
handle_command(round, State) ->
    {reply, State#state.round, ignore};
handle_command(next_round, State) ->
    {reply, ok, [], State#state{round=State#state.round+1}};
handle_command(seqmap, State) ->
    {reply, State#state.seqmap, ignore};
handle_command(is_done, State) ->
    {reply, State#state.got_hello andalso State#state.got_self_hai, ignore};
handle_command(was_saluted, State) ->
    {reply, State#state.got_salutation, ignore};
handle_command(Msg, _State) ->
    ct:pal("catchall handle_command, Msg: ~p", [Msg]),
    {reply, ok, ignore}.

handle_message(<<"seq", Int:8/integer>>, Actor, State = #state{seqmap=Seqmap}) ->
    ct:pal("seq ~p from ~p", [Int, Actor]),
    Current = maps:get(Actor, Seqmap, 0),
    case  Current + 1 of
        Int when Int =< State#state.round ->
            {State#state{seqmap=maps:put(Actor, Int, Seqmap)}, []};
        X when X > Current ->
            %% valid, but not yet
            defer;
        _Other ->
            %% invalid, drop it
            ignore
    end;
handle_message(<<"greet">>, _Actor, State) ->
    {State, [{callback, <<"greet">>}]};
handle_message(<<"salute">>, _Actor, State) ->
    {State, [{callback, <<"salute">>}]};
handle_message(<<"hello">>, Actor, State) ->
    {State#state{got_hello=true}, [{unicast, Actor, <<"ehlo">>}, {multicast, <<"hai">>}]};
handle_message(<<"hai">>, Actor, State = #state{id=Actor}) ->
    {State#state{got_self_hai=true}, []};
handle_message(<<"unicast: ", Msg/binary>>, Actor, State) ->
    {State, [{unicast, Actor, Msg}]};
handle_message(<<"salutations to ", ID/binary>>, Actor, State = #state{id=Actor}) ->
    case list_to_integer(binary_to_list(ID)) of
        Actor ->
            {State#state{got_salutation=true}, []};
        _ ->
            ignore
    end;
handle_message(Msg, Actor, State) ->
    ct:pal("handle_message, Msg: ~p, Actor: ~p~n", [Msg, Actor]),
    {State, []}.

callback_message(Actor, <<"greet">>, #state{id=Actor}) ->
    %% normal people don't greet themselves
    none;
callback_message(Actor, <<"greet">>, _State) ->
    list_to_binary(io_lib:format("greetings to ~b", [Actor]));
callback_message(Actor, <<"salute">>, _State) ->
    list_to_binary(io_lib:format("salutations to ~b", [Actor])).


serialize(State) ->
    %%ct:pal("Serialize: ~p~n", [State]),
    term_to_binary(State).

deserialize(Binary) ->
    %%ct:pal("Deserialize: ~p~n", [Binary]),
    binary_to_term(Binary).

restore(OldState, NewState) ->
    ct:pal("OldState: ~p, NewState: ~p~n", [OldState, NewState]),
    {ok, OldState}.
