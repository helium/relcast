-module(fakecast).

-export([start_test/4]).

-callback init(term()) -> {ok, Settings :: settings(), InitialState :: term()}.
-callback model(Message :: message(),
                From :: pos_integer(),
                To :: pos_integer(),
                NodeState :: term(),
                NewState :: term(),
                Actions :: [term()],
                ModelState :: term()) -> model_output().
-callback terminate(Reason :: term(), NewState :: term()) -> any().
-optional_callbacks([terminate/2]).

-type settings() :: {
                     ModuleUnderTest :: atom(),
                     ModelFun :: function(),
                     MessageOrdering :: ordering(),
                     TimeStrategy :: strategy(),
                     NodeList :: [node_id()],
                     NodeConfig :: [{name(), init_state(), term()}],
                     MaxTime :: pos_integer()
                    }.

-type ordering() :: round_robin | random. %% others?  attack_leader?  nemesis?
-type strategy() :: favor_serial | favor_concurrent.  %% time
-type init_state() :: started | stopped.

-type message() ::
        {unicast, Index::pos_integer(), Msg::message_value()} |
        {multicast, Msg::message_value()}.

-type message_key_prefix() :: <<_:128>>.
-type message_value() ::
        {message_key_prefix(), binary()} | binary().

-type model_output() :: fail | {fail, Reason :: term()} | success | %% final states
                        continue | % take no action
                        drop | reorder | {insert, Msg :: term() } | %% message actions
                        {insert_input, ID :: node_id(), Input :: term()} |
                        {alter_state, ID :: node_id(), NewState :: term()} |
                        {alter_actions, ID :: node_id(), NewActions :: [term()]} |
                        {stop_node, ID :: name() | node_id()} |
                        {restart_node, ID :: name() | node_id()} |
                        {start_node, ID :: name() | node_id()}.

-type name() :: atom().
-type node_id() :: pos_integer().

-type input() :: {node_id() | all, term()}.

%% fakecast is a drop-in replacement runner for relcast protocols that
%% allows the user to easily specify test cases that go beyond simply
%% running protocol and attempting to intervene.  it serializes the
%% protocol, and allows message mutation and filtering via global
%% policies and targeted callbacks.

%% TODO: figure out how to wire this so that EQC can generate and
%% shrink a command history so that we can generate automatic test cases.

%% TODO: somehow allow nodes to behave in a byzantine manner, either
%% by state-mutating callbacks or alternate implementations that
%% attack the protocol or something else??a

%% how this looks: we should be able to set up a test with initial
%% conditions like random seed, node count (names?), round count, end
%% condition, etc.

-spec start_test(atom(), term(), rand:seed(), [input()]) -> term().
start_test(TestModule, TestArgs, Seed, InitialInput) ->
    %% establish the seed for repeatability
    case Seed of
        {Mod, ModSeed} ->
            rand:seed(Mod, ModSeed);
        _ ->
            rand:seed(exs1024s, Seed)
    end,

    {ok,
     {
      Module,
      _MessageOrdering,
      _TimeStrategy,
      Nodes,
      Configs,
      _MaxTime
     } = TestConfig,
     TestState} = TestModule:init(TestArgs),

    %% initialize all the nodes
    case length(Nodes) == length(Configs) of
        true -> ok;
        false -> throw(configs_mismatch)
    end,

    {MaxID, NodeStates} =
        lists:foldl(fun({_NodeName, NodeArgs}, {NodeID, Acc}) ->
                            NodeState = erlang:apply(Module, init, NodeArgs),
                            {NodeID + 1, Acc#{NodeID => NodeState}}
                    end, {1, #{}}, lists:zip(Nodes, Configs)),

    %% separate input queues to make scheduling simpler
    Queues = maps:from_list([{N, []}|| N <- lists:seq(1, MaxID - 1)]),

    %% refactor this into a fold to support multiple initial inputs
    [{Target, Message}] = InitialInput,
    #{Target := TargetState} = NodeStates,

    ct:pal("providing ~p as initial input to ~p", [Message, Target]),

    {Queues1, NodeStates1} = process_output(Module:input(TargetState, Message),
                                            Target,
                                            Queues,
                                            NodeStates),

    SeedNode = rand:uniform(length(Nodes)),
    %% time in abstract units
    CurrentTime = 0,
    Timers = #{},

    ct:pal("init complete, entering test loop"),

    test_loop(TestConfig, TestModule, SeedNode, CurrentTime, Timers,
              NodeStates1, Queues1, TestState).

test_loop({Module, Ordering, Strategy, _, _, MaxTime} = TestConfig, Test,
          PrevNode, PrevTime, Timers, NodeStates, Queues, TestState) ->

    Node = sched(Ordering, PrevNode, maps:size(Queues)),

    Time = advance_time(Strategy, PrevTime),

    case Time >= MaxTime of
        true ->
            ct:pal("too much abstract time has passed.  test state:~n"
                   "~p~n~p~n~p~n", [TestState, Queues, NodeStates]),
            throw(etoomuchtime);
        _ -> ok
    end,

    case Time rem 1000 of
        0 when Time /= 0 ->
            ct:pal("time marker: ~p", [Time]);
        _ -> meh
    end,

    %% implement this
    ok = process_timers(Time, Timers),

    case maps:get(Node, Queues, []) of
        [] ->
            test_loop(TestConfig, Test, Node, Time, Timers, NodeStates, Queues, TestState);
        [{From, Message}|T] ->
            #{Node := NodeState} = NodeStates,

            {NewState, Actions} = Module:handle_msg(NodeState, From, Message),

            %% ct:pal("delivering message ~p to node ~p resulted in ~p",
            %%        [element(1, Message), Node, Actions]),

            case Test:model(Message, From, Node, NodeState, NewState, Actions, TestState) of
                success -> ok;
                {result, Result} -> {ok, Result};
                fail -> throw(fakecast_model_failure);
                {fail, Reason} -> throw({fakecast_model_failure, Reason});
                {continue, TestState1} ->
                    {Queues1, NodeStates1} = process_output({NewState, Actions},
                                                             Node,
                                                             Queues#{Node => T},
                                                             NodeStates),
                    test_loop(TestConfig, Test, Node, Time, Timers, NodeStates1, Queues1, TestState1)
            end
    end.

process_output({NewState, {send, Messages}}, Current, Queues, States) ->
    %% add messages to queues
    Queues1 = lists:foldl(fun(Message, Acc) ->
                                  case Message of
                                      {unicast, Target, Msg} ->
                                          #{Target := TargetQueue} = Acc,
                                          Acc#{Target => TargetQueue ++ [{Current, Msg}]};
                                      Msg ->
                                          throw({unhandled, Msg, Acc})
                                  end
                          end,
                          Queues,
                          Messages),
    {Queues1, States#{Current => NewState}};
process_output({NewState, Ign}, Current, Queues, States) when Ign == ok;
                                                              Ign == ignore ->
    {Queues, States#{Current => NewState}};
process_output({NewState, {result, _}}, Current, Queues, States) ->
    {Queues, States#{Current => NewState}};
process_output(Output, _, _, _) ->
    throw({unknown_output, Output}).


%% this should maybe be a list?
%% handle_model_result(F, _Q, _S) when F == fail orelse
%%                                   element(1, F) == fail orelse
%%                                   F == success ->
%%     {F, _Q, _S};
%% handle_model_result(continue, Q, S) ->
%%     {continue, Q, S}.


%%%% helpers

sched(round_robin, Current, Size) ->
    ((Current + 1) rem Size) + 1;
sched(random, _Current, Size) ->
    rand:uniform(Size).

advance_time(favor_concurrent, Time) ->
    case rand:uniform(100) of
        N when N =< 65 ->
            Time;
        N when N =< 99 ->
            Time + 1;
        _ ->
            Time + 2
    end;
advance_time(favor_sequential, Time) ->
    case rand:uniform(100) of
        N when N =< 30 ->
            Time;
        N when N =< 60 ->
            Time + 1;
        N when N =< 99 ->
            Time + 2;
        _ ->
            Time + 3
    end.

process_timers(_Time, _Timers) ->
    ok.
