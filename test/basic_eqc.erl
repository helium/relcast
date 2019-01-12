-module(basic_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile([export_all, nowarn_export_all]).

-behaviour(eqc_statem).
-behaviour(relcast).

%% -- State and state functions ----------------------------------------------

-record(act,
        {
         id :: non_neg_integer(),
         sent = 0 :: non_neg_integer(),
         acked = 0 :: non_neg_integer()
        }).

-type act() :: #act{}.

-record(s,
        {
         rc :: term(),
         dir :: string(),
         actors :: [pos_integer()],
         act_st = #{} :: #{pos_integer() => act()},
         inflight = #{} :: #{pos_integer() => term()},
         messages = #{} :: #{{pos_integer, pos_integer()} => term},
         running = false :: boolean()
        }).

-define(M, ?MODULE).

%% this module describes a simple relcast setup of extremely simple
%% actors that send and receive random messages.  The actors are
%% virtual, in that they don't have any pids themselves, hence no
%% nondeterminism.  Todos:
%%  - two relcast instances talking to each other (if possible in the
%%    same VM)
%%  - PULSE test
%%  - non-virtual actors expressing some simple protocol

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    Actors = lists:seq(2, 2),

    States = maps:from_list([{A, #act{id = A}}
                             || A <- Actors]),

    Inf = maps:from_list([{A, []}
                             || A <- Actors]),

    #s{actors = Actors,
       rc = undefined,
       inflight = Inf,
       running = true,
       act_st = States}.

%% -- Generators -------------------------------------------------------------

command(S) ->
    frequency(
      [
       {1, {call, ?M, open, [S#s.actors, S#s.dir]}},
       {1, {call, ?M, close, [S#s.rc]}},
       {15, {call, ?M, command, [S#s.rc, oneof(S#s.actors), ?SUCHTHAT(X, binary(), byte_size(X) > 0)]}},
       {10, {call, ?M, take, [S#s.rc, oneof(S#s.actors)]}},
       %{10, {call, ?M, peek, [S#s.rc, oneof(S#s.actors)]}},
       {10, {call, ?M, ack, [S#s.rc, oneof(S#s.actors), S#s.act_st, S#s.inflight]}},
       {2, {call, ?M, ack_all, [S#s.rc, oneof(S#s.actors), S#s.act_st, S#s.inflight]}},
       {2, {call, ?M, reset, [S#s.rc, oneof(S#s.actors)]}}
      ]).


precondition(S, {call, _, open, _}) ->
     S#s.rc == undefined orelse S#s.running == false;
precondition(S, {call, _, close, _}) ->
    S#s.rc /= undefined andalso S#s.running == true;
precondition(S, _) ->
    S#s.rc /= undefined andalso S#s.running == true.

dynamic_precondition(_S, {_, _, ack, [_, Actor, _, Inf]}) ->
    #{Actor := Q} = Inf,
    Q /= [];
dynamic_precondition(_S, _) ->
    true.

postcondition(S, {call, _, take, [_, Actor]}, R) ->
    io:format("take for ~p ~p~n", [Actor, S#s.messages]),
    MsgInFlight = length(maps:get(Actor, S#s.inflight, [])) + 1,
    #act{acked=Acked} = maps:get(Actor, S#s.act_st),
    Expected = case MsgInFlight >= 20 of
        true ->
            pipeline_full;
        false ->
            maps:get({Actor, MsgInFlight+Acked}, S#s.messages, not_found)
    end,

    case R of
        {ok, _Seq, Expected, _RC} ->
            io:format("~p took ~p~n", [Actor, Expected]),
            true;
        {Expected, _RC} ->
            io:format("~p took ~p~n", [Actor, Expected]),
            true;
        {ok, _Seq, Unexpected, _RC} ->
            {unexpected_take1, Actor, Expected, Unexpected, S#s.messages, S#s.inflight};
        {Unexpected, _RC} ->
            {unexpected_take2, Actor, Expected, Unexpected, S#s.messages, S#s.inflight}
    end;
postcondition(S, {call, _, peek, [_, Actor]}, R) ->
    io:format("peek for ~p ~p~n", [Actor, S#s.messages]),
    MsgInFlight = length(maps:get(Actor, S#s.inflight, [])) + 1,
    #act{acked = Acked} = maps:get(Actor, S#s.act_st),
    Expected = maps:get({Actor, MsgInFlight+Acked}, S#s.messages, not_found),

    case R of
        Expected ->
            true;
        {ok, Expected}->
            true;
        {ok, Unexpected}->
            {unexpected_peek, Actor, {exp, Expected}, {got, Unexpected},
             S#s.messages, S#s.inflight}
    end;
postcondition(_, _, _) ->
    true.

next_state(S, Res, {call, _, open, _}) ->
     S#s{rc = {call, erlang, element, [1, Res]}, dir = {call, erlang, element, [2, Res]}, running = true};
next_state(S, _, {call, _, close, _}) ->
    Inf = maps:from_list([{A, []}
                             || A <- S#s.actors]),
    S#s{running=false, inflight=Inf};
next_state(#s{messages = Msgs,
              act_st = States} = S,
           RC, {_, _, command, [_RC0, Actor, Msg]}) ->
    S#s{rc = RC,
        messages = {call, ?M, command_messages, [Actor, States, Msg, Msgs]},
        act_st = {call, ?M, command_states, [Actor, States]}};
next_state(#s{act_st = States, inflight=InFlight} = S,
           RC,
           {_, _, ack, [_, Actor, _, _]}) ->
    S#s{rc = RC,
        act_st = {call, ?M, ack_states, [Actor, States, InFlight]},
        inflight = {call, ?M, deliver_inf, [Actor, InFlight]}};
next_state(#s{act_st = States, inflight = InFlight} = S,
           RC,
           {_, _, ack_all, [_, Actor, _, _]}) ->
    S#s{rc = RC,
        act_st = {call, ?M, ack_all_states, [Actor, States, InFlight]},
        inflight = {call, ?M, reset_inf, [Actor, InFlight]}};
next_state(#s{act_st = States,
              inflight = Inf} = S,
           _V, {_, _, deliver, [Actor]}) ->
    S#s{act_st = {call, ?M, deliver_st, [Inf, Actor, States]},
        inflight = {call, ?M, deliver_inf, [Actor, Inf]}};
next_state(S, V, {_, _, take, [_, Actor]}) ->
    S#s{rc = {call, ?M, extract_state, [V]},
        inflight = {call, ?M, extract_inf, [V, Actor, S#s.inflight]}};
next_state(S, _V, {_, _, peek, _}) ->
    S;
next_state(S, RC, {_, _, reset, [_, Actor]}) ->
    S#s{rc = RC,
        inflight = {call, ?M, reset_inf, [Actor, S#s.inflight]}};
next_state(S, _V, _C) ->
    error({S, _V, _C}),
    S.

command_states(Actor, States) ->
    #{Actor := St = #act{sent = Sent}} = States,
    States#{Actor => St#act{sent = Sent + 1}}.

command_messages(Actor, States, Msg, Msgs) ->
    #{Actor := #act{sent = Sent}} = States,
    Msgs#{{Actor, Sent + 1} => Msg}.

ack_states(Actor, States, InFlight) ->
    io:format("~p in flight ~p~n", [Actor, InFlight]),
    MyInFlight = maps:get(Actor, InFlight),
    #{Actor := State} = States,
    case State of
        #act{sent = Sent, acked = Acked} when Sent == Acked ->
            States;
        #act{acked = Acked} when length(MyInFlight) > 0 ->
            io:format("incrementing acks~n"),
            States#{Actor => State#act{acked = Acked + 1}};
        _ ->
            States
    end.

ack_all_states(Actor, States, InFlight) ->
    MyInFlight = maps:get(Actor, InFlight),
    #{Actor := State} = States,
    case State of
        #act{sent = Sent, acked = Acked} when Sent == Acked ->
            States;
        #act{} when length(MyInFlight) > 0 ->
            io:format("incrementing acks~n"),
            {Seq, _} = lists:last(MyInFlight),
            States#{Actor => State#act{acked = Seq + 1}};
        _ ->
            States
    end.

deliver_st(Inf, Actor, States) ->
    case Inf of
        #{Actor := [_H|_T]} ->
            #{Actor := #act{sent = Sent} = State} = States,
            States#{Actor => State#act{sent = Sent + 1}};
        _ ->
            States
    end.

deliver_inf(Actor, Inf) ->
    case Inf of
        #{Actor := [_H|T]} ->
            Inf#{Actor => T};
        _ ->
            Inf
    end.


reset_inf(Actor, Inf) ->
    Inf#{Actor => []}.

extract_state({_, RC}) ->
    RC;
extract_state({ok, _, _, RC}) ->
    RC.

extract_inf({_, _}, _, Inf) ->
    Inf;
extract_inf({ok, Seq, Msg, _RC}, Actor, Inf) ->
    #{Actor := Q} = Inf,
    Inf#{Actor => Q ++ [{Seq, Msg}]}.

%% -- Commands ---------------------------------------------------------------

open(Actors, Dir0) ->
    Dir = case Dir0 of
              undefined ->
                  DirNum = erlang:unique_integer(),
                  D = "data/data-" ++ integer_to_list(DirNum),
                  os:cmd("rm -rf "++D),
                  D;
              Dir0 ->
                  Dir0
          end,
    {ok, RC} = relcast:start(1, [1 | Actors], ?M, [], [{data_dir, Dir}]),
    {RC, Dir}.

close(RC) ->
    relcast:stop(reason, RC).

command(RC, Actor, Msg) ->
    {ok, RC1} = relcast:command({Actor, Msg}, RC),
    RC1.

take(RC, Actor) ->
    relcast:take(Actor, RC).

peek(RC, Actor) ->
    relcast:peek(Actor, RC).

%% note that this simulates a deliver to a remote actor; we never
%% actually make or deliver any messages to the local relcast instance.
deliver(_Actor) ->
    ok.

ack(RC, Actor, States, InFlight) ->
    #{Actor := State} = States,
    case State of
        #act{sent = Sent, acked = Acked} when Sent == Acked ->
            RC1 = RC;
        _ ->
            OurInFlight = maps:get(Actor, InFlight),
            case length(OurInFlight) > 0 of
                false ->
                    RC1 = RC;
                true ->
                    {Seq, _} = hd(OurInFlight),
                    {ok, RC1} = relcast:ack(Actor, Seq, RC)
            end
    end,
    RC1.

ack_all(RC, Actor, States, InFlight) ->
    #{Actor := State} = States,
    case State of
        #act{sent = Sent, acked = Acked} when Sent == Acked ->
            RC1 = RC;
        _ ->
            OurInFlight = maps:get(Actor, InFlight),
            case length(OurInFlight) > 0 of
                false ->
                    RC1 = RC;
                true ->
                    {Seq, _} = lists:last(OurInFlight),
                    {ok, RC1} = relcast:ack(Actor, Seq, RC)
            end
    end,
    RC1.

reset(RC, Actor) ->
    {ok, RC1} = relcast:reset_actor(Actor, RC),
    RC1.


cleanup(#s{rc=undefined}) ->
    ok;
cleanup(#s{rc=RC, dir=Dir, running=Running}) ->
    case Running of
        true ->
            catch relcast:stop(reason, RC);
        false ->
            ok
    end,
    os:cmd("rm -rf " ++ Dir).

%% -- Property ---------------------------------------------------------------
prop_basic() ->
    ?FORALL(
       Cmds, %?SIZED(20,
       commands(?M),%)

       begin
           {H, S, Res} = run_commands(Cmds),
           ?WHENFAIL(begin
                         io:format("~p~n", [Res]),
                         io:format("~p~n", [eqc_symbolic:eval(S)]),
                         eqc_statem:pretty_commands(?M,
                                      Cmds,
                                      {H, S, Res},
                                      cleanup(eqc_symbolic:eval(S)))
                     end,
           Res == ok)
       end).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Runs for 10 seconds before giving up finding more bugs.
-spec bugs() -> [eqc_statem:bug()].
bugs() -> bugs(10).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Runs for N seconds before giving up finding more bugs.
-spec bugs(non_neg_integer()) -> [eqc_statem:bug()].
bugs(N) -> bugs(N, []).

%% @doc Run property repeatedly to find as many different bugs as
%% possible. Takes testing time and already found bugs as arguments.
-spec bugs(non_neg_integer(), [eqc_statem:bug()]) -> [eqc_statem:bug()].
bugs(Time, Bugs) ->
    more_bugs(eqc:testing_time(Time, prop_basic()), 20, Bugs).


%% local relcast impl

init(_) ->
    {ok, []}.

handle_command({Actor, Msg}, State) ->
    {reply, ok, [{unicast, Actor, Msg}], State}.

serialize(Foo) ->
    term_to_binary(Foo).

deserialize(Foo) ->
    binary_to_term(Foo).

restore(A, _B) ->
    {ok, A}.

handle_message(_, _, _) ->
    ignore.

callback_message(_, _, _) ->
    ignore.
