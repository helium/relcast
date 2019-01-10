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


%% this module describes a simple relcast setup of extremely simple
%% actors that send and receive random messages.  The actors are
%% virtual, in that they don't have any pids themselves, hence no
%% nondeterminism.  Todos:
%%  - two relcast instances talking to each other (if possible in the
%%    same VM
%%  - PULSE test
%%  - non-virtual actors expressing some simple protocol

%% @doc Returns the state in which each test case starts. (Unless a different
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    Actors = lists:seq(2, 3),
    DirNum = erlang:unique_integer(),
    Dir = "data-" ++ integer_to_list(DirNum),

    States = maps:from_list([{A, #act{id = A}}
                             || A <- Actors]),

    Inf = maps:from_list([{A, []}
                             || A <- Actors]),

    %% add one here, but it's local so we don't want it selected for deliveries.
    {ok, RC} = relcast:start(1, [1 | Actors], ?MODULE, [], [{data_dir, Dir}]),

    #s{actors = Actors,
       rc = RC,
       inflight = Inf,
       running = true,
       act_st = States,
       dir = Dir}.

%% -- Generators -------------------------------------------------------------

command(S) ->
    frequency(
      [
       %% {1, {call, ?MODULE, open, [S#s.actors, S#s.dir]}},
       %% {1, {call, ?MODULE, close, [S#s.rc]}},
       {10, {call, ?MODULE, command, [S#s.rc, oneof(S#s.actors), binary()]}},
       {10, {call, ?MODULE, take, [S#s.rc, oneof(S#s.actors)]}},
       {10, {call, ?MODULE, deliver, [oneof(S#s.actors)]}},
       %% {10, {call, ?MODULE, peek, [S#s.rc, oneof(S#s.actors)]}},
       {10, {call, ?MODULE, ack, [S#s.rc, oneof(S#s.actors), S#s.act_st]}}%,
       %% {1, {call, ?MODULE, reset, [S#s.rc, oneof(S#s.actors)]}}
      ]).

%% -- Common pre-/post-conditions --------------------------------------------
%% @doc General command filter, checked before a command is generated.
%% -spec command_precondition_common(S, Cmd) -> boolean()
%%     when S    :: eqc_statem:symbolic_state(),
%%          Cmd  :: atom().
%% command_precondition_common(#s{running = false}, {call, _, open, _}) ->
%%     true;
%% command_precondition_common(#s{running = false}, _) ->
%%     false;
%% command_precondition_common(_, _) ->
%%     true.

%% precondition(S, {call, _, open, _}) when S#s.rc == undefined orelse
%%                                          S#s.running == false ->
%%     true;
%% precondition(S, _) when S#s.rc == undefined orelse
%%                         S#s.running == false ->
%%     false;
%% precondition(#s{inflight = #{}}, {call, _, deliver, _}) ->
%%     false;
precondition(_, _) ->
    true.

postcondition(_, _, _) ->
    true.

%% next_state(S, {ok, Ref}, {call, _, open, _}) ->
%%     S#s{rc = Ref, running = true};
next_state(#s{messages = Msgs,
              act_st = States} = S,
           RC, {_, _, command, [_RC0, Actor, Msg]}) ->
    #{Actor := St = #act{sent = Sent}} = States,
    S#s{rc = RC,
        messages = Msgs#{{Actor, Sent + 1} => Msg},
        act_st = States#{Actor => St#act{sent = Sent + 1}}};
next_state(#s{act_st = States} = S,
           RC,
           {_, _, ack, [_, Actor, _]}) ->
    #{Actor := State} = States,
    States1 =
        case State of
            #act{sent = Sent, acked = Acked} when Sent == Acked ->
                States;
            #act{acked = Acked} ->
                States#{Actor => State#act{acked = Acked + 1}}
        end,
    S#s{rc = RC,
        act_st = States1};
next_state(#s{act_st = States,
              inflight = Inf} = S,
           _V, {_, _, deliver, [Actor]}) ->
    S#s{act_st = {call, ?MODULE, deliver_st, [Inf, Actor, States]},
        inflight = {call, ?MODULE, deliver_inf, [Actor, Inf]}};
next_state(S, V, {_, _, take, [_, Actor]}) ->
    S#s{rc = {call, ?MODULE, extract_state, [V]},
        inflight = {call, ?MODULE, extract_inf, [V, Actor, S#s.inflight]}};
next_state(S, _V, _C) ->
    error({S, _V, _C}),
    S.

extract_state({_, RC}) ->
    RC;
extract_state({ok, _, _, RC}) ->
    RC.

extract_inf({_, _}, _, Inf) ->
    Inf;
extract_inf({ok, _Seq, Msg, _RC}, Actor, Inf) ->
    #{Actor := Q} = Inf,
    Inf#{Actor => Q ++ [Msg]}.

deliver_states(Inf, Actor, States) ->
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

%% -- Commands ---------------------------------------------------------------

open(Actors, Dir) ->
    relcast:start(1, [1 | Actors], ?MODULE, [], [{data_dir, Dir}]).

command(RC, Actor, Msg) ->
    {ok, RC1} = relcast:command({Actor, Msg}, RC),
    RC1.

take(RC, Actor) ->
    relcast:take(Actor, RC).

%% note that this simulates a deliver to a remote actor; we never
%% actually make or deliver any messages to the local relcast instance.
deliver(_Actor) ->
    ok.

ack(RC, Actor, States) ->
    #{Actor := State} = States,
    case State of
        #act{sent = Sent, acked = Acked} when Sent == Acked ->
            RC1 = RC;
        #act{acked = Acked} ->
            {ok, RC1} = relcast:ack(Actor, Acked + 1, RC)
    end,
    RC1.

cleanup(undefined) ->
    ok;
cleanup(RC) ->
    relcast:stop(reason, RC).

%% -- Property ---------------------------------------------------------------
prop_basic() ->
    ?FORALL(
       Cmds, %?SIZED(20,
       commands(?MODULE),%)

       begin
           {H, S, Res} = run_commands(Cmds),
           eqc_statem:pretty_commands(?MODULE,
                                      Cmds,
                                      {H, S, Res},
                                      cleanup(S#s.rc))
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
