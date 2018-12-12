-module(fakecast).

-include_lib("kernel/include/logger.hrl").
-include("fakecast.hrl").

-export([trace/1, trace/2,
         ptest/3,
         start_test/4,
         start_test/5]).

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

-type settings() :: #fc_conf{}.

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
                        {partition_node, ID :: name() | node_id()} |
                        {restart_node, ID :: name() | node_id()} |
                        {start_node, ID :: name() | node_id()}.

-type input() :: {node_id() | all, term()}.

-type seed() :: {integer(), integer(), integer()}.

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

-record(node,
        {
         name :: atom(),
         queue = [] :: [term()],
         dqueue = [] :: [term()],
         status = running :: running | stopped | partitioned,
         timer = undefined :: undefined | pos_integer(),
         state :: term()
        }).

trace(Format) ->
    trace(Format, []).

trace(Format, Args) ->
    Time = erlang:get(ab_time),
    Node = erlang:get(curr_node),
    File = erlang:get(trace_file),
    file:write(File,
               io_lib:format("~4B:~2B|" ++ Format ++ "~n",
                             [Time, Node] ++ Args)).

%% helper for testing at the shell
ptest(Pids, Tests, Fun) ->
    Me = self(),
    [spawn(fun() ->
                   ptest_loop(Me, Tests, Fun)
           end)
     || _ <- lists:seq(1,Pids)],
    [receive
         ok -> ok;
         {error, E} ->
             io:format("execution ended with non-match ~p", [E])
     end
     || _ <- lists:seq(1,Pids)].

ptest_loop(Me, 0, _Fun) ->
    Me ! ok;
ptest_loop(Me, Tests, Fun) ->
    case (catch Fun()) of
        ok ->
            ptest_loop(Me, Tests - 1, Fun);
         E ->
            Me ! {error, E}
    end.

-spec start_test(fun(), fun(), seed(), [input()] | fun()) -> term().
start_test(Init, Model, Seed, InitialInput) ->
    start_test(Init, Model, Seed, InitialInput, #{}).

-spec start_test(fun(), fun(), seed(), [input()] | fun(), Options :: #{}) -> term().
start_test(Init, Model, Seed, InitialInput0, Options) ->
    %% establish the seed for repeatability
    SeedStr =
        case Seed of
            {Mod, ModSeed} ->
                rand:seed(Mod, ModSeed),
                seed_to_string(ModSeed);
            _ ->
                rand:seed(exs1024s, Seed),
                seed_to_string(Seed)
        end,

    TraceFileName = maps:get(trace_file_name, Options, "trace-" ++ os:getpid() ++ "-" ++ SeedStr ),
    {ok, TraceFile} = file:open(TraceFileName, [write, delayed_write]),
    erlang:put(trace_file, TraceFile),

    %% set up crap for early traces
    erlang:put(curr_node, 0),
    erlang:put(ab_time, 0),

    {ok,
     #fc_conf{
        test_mod = Module,
        nodes = NodeNames,
        configs = Configs,
        id_start = IDStart
       } = TestConfig,
     TestState} = Init(),

    %% initialize all the nodes
    case length(NodeNames) == length(Configs) of
        true -> ok;
        false -> throw(configs_mismatch)
    end,

    {_MaxID, Nodes} =
        lists:foldl(fun({NodeName, NodeArgs}, {NodeID, Acc}) ->
                            NodeState = erlang:apply(Module, init, NodeArgs),
                            {NodeID + 1, Acc#{NodeID => #node{state = NodeState,
                                                              name = NodeName}}}
                    end, {IDStart, #{}}, lists:zip(NodeNames, Configs)),

    %% time in abstract units
    CurrentTime = 0,

    InitialInput =
        case InitialInput0 of
            Fun when is_function(Fun) ->
                Fun();
            List when is_list(List) ->
                List;
            _ ->
                throw(bad_input)
        end,

    Nodes1 =
        lists:foldl(
          fun({Target, Message}, Ns) ->
                  #{Target := #node{state = TargetState}} = Ns,
                  trace("providing ~p as initial input to ~p", [Message, Target]),

                  process_output(Module:input(TargetState, Message), Message,
                                 Target,
                                 CurrentTime,
                                 Ns)
          end,
          Nodes,
          InitialInput),

    SeedNode = rand:uniform(maps:size(Nodes1)),

    trace("init complete, entering test loop, seed is ~p", [Seed]),

    test_loop(TestConfig, Model,
              SeedNode, CurrentTime,
              Nodes1, TestState).

test_loop(#fc_conf{test_mod = Module,
                   ordering = Ordering,
                   strategy = Strategy,
                   id_start = IDStart,
                   max_time = MaxTime} = TestConfig,
          Model,
          PrevNode, PrevTime,
          Nodes, TestState) ->

    Node = sched(Ordering, PrevNode, maps:size(Nodes), IDStart),
    erlang:put(curr_node, Node),

    Time = advance_time(Strategy, PrevTime),
    erlang:put(ab_time, Time),

    case Time >= MaxTime of
        true ->
            %% eventually I'd like to move to logger but there are
            %% issues right now, hence the duplication
            trace("too much abstract time has passed.  test state:~n"
                  "~p~n~s~n~n", [TestState, format_nodes(Nodes)]),
            ?LOG_ERROR("too much abstract time has passed.  test state:~n"
                       "~p~n~s~n~n", [TestState, format_nodes(Nodes)]),
            throw(etoomuchtime);
        _ -> ok
    end,

    Nodes1 = process_timers(Time, Nodes),

    %% perhaps at this point we should do a quick check for empty
    %% timers and empty queues (at least occasionally).  if we're in
    %% that state, we're inevitably going to time out, as there's
    %% nothing to drive forward progress.

    case maps:get(Node, Nodes1) of
        #node{queue = [], dqueue = []} ->
            test_loop(TestConfig, Model, Node, Time, Nodes1, TestState);
        #node{status = SorP} when SorP =:= stopped;
                                  SorP =:= partitioned ->
            %% trace("node ~p is ~p", [Node, SorP]),
            test_loop(TestConfig, Model, Node, Time, Nodes1, TestState);
        #node{queue = Queue, dqueue = DQueue, state = NodeState} ->
            %% trace("node ~p is ~p", [Node, Status]),
            {From, Message, Queue1, DQueue1} = next_message(Queue, DQueue),
            {NewState, Actions} =
                case Module:handle_msg(NodeState, From, Message) of
                    {S, A} -> {S, A};
                    ignore -> {NodeState, ignore}
                end,

            %% ideally this is 100% statically aligned up to the actions
            trace("~-16s ~2B->~2B => ~s",
                  [print_message(Message),
                   From, Node,
                   print_actions(Actions)]),
            %% trace("~p", [Message]),

            case Model(Message, From, Node, NodeState, NewState, Actions, TestState) of
                success -> file_close(erlang:get(trace_file)), ok;
                {result, Result} -> file_close(erlang:get(trace_file)), {ok, Result};
                fail -> file_close(erlang:get(trace_file)), throw(fakecast_model_failure);
                {fail, Reason} -> file_close(erlang:get(trace_file)), throw({fakecast_model_failure, Reason});
                {actions, TestActions, TestState1} ->
                    {Nodes2, NewState1, Actions1} =
                        process_actions(TestActions, Nodes1, NewState, Actions),
                    #{Node := NodeSt} = Nodes2,
                    Nodes3 =
                        process_output({NewState1, Actions1}, {From, Message},
                                       Node, Time,
                                       Nodes2#{Node => NodeSt#node{queue = Queue1,
                                                                   dqueue = DQueue1}}),
                    test_loop(TestConfig, Model, Node, Time, Nodes3, TestState1)
            end
    end.

next_message([], [{From, Message} | T]) ->
    {From, Message, [], T};
next_message([{From, Message} | T], []) ->
    {From, Message, T, []};
next_message(Queue, DQueue) ->
    case rand:uniform(2) of
        1 ->
            [{From, Message} | T] = Queue,
            {From, Message, T, DQueue};
        2 ->
            [{From, Message} | T] = DQueue,
            {From, Message, Queue, T}
    end.


process_output({NewState, {send, Messages}}, _, Current, _Time, Nodes) ->
    #{Current := Node} = Nodes,
    %% add messages to queues
    send_messages(Current,
                  Nodes#{Current := Node#node{state = NewState}},
                  Messages);
process_output({NewState, {result_and_send, _Result, {send, Messages}}}, _, Current, _Time, Nodes) ->
    #{Current := Node} = Nodes,
    %% add messages to queues
    send_messages(Current,
                  Nodes#{Current := Node#node{state = NewState}},
                  Messages);
process_output({NewState, Ign}, _, Current, _Time, Nodes) when Ign == ok;
                                                            Ign == ignore ->
    #{Current := Node} = Nodes,
    Nodes#{Current := Node#node{state = NewState}};
process_output({NewState, {result, _}}, _, Current, _Time, Nodes) ->
    #{Current := Node} = Nodes,
    Nodes#{Current := Node#node{state = NewState}};
process_output({NewState, start_timer}, _, Current, Time, Nodes) ->
    trace("starting timer ~p for ~p", [Time + 1000, Current]),
    #{Current := Node} = Nodes,
    Nodes#{Current := Node#node{timer = Time + 1000,
                                state = NewState}};
process_output({NewState, defer}, {From, Message}, Current, _Time, Nodes) ->
    #{Current := #node{dqueue = DQueue} = Node} = Nodes,
    DQueue1 = DQueue ++ [{From, Message}],
    Nodes#{Current := Node#node{dqueue = DQueue1, state = NewState}};
process_output(Output, _, _, _, _) ->
    throw({unknown_output, Output}).


process_actions([], Nodes, State, Actions) ->
    {Nodes, State, Actions};
process_actions([Action|T], Nodes, State, Actions) ->
    {Nodes1, State1, Actions1} = process_action(Action, Nodes, State, Actions),
    process_actions(T, Nodes1, State1, Actions1).

process_action({stop_node, ID}, Nodes, State, Actions) ->
    trace("stopping node ~p", [ID]),
    #{ID := Node} = Nodes,
    {Nodes#{ID => Node#node{status = stopped}},
     State, Actions};
process_action({alter_state, NewState}, Nodes, _State, Actions) ->
    trace("altering state of node"),
    {Nodes, NewState, Actions};
process_action({alter_actions, NewActions}, Nodes, State, Actions) ->
    trace("alter actions ~s to ~s", [print_actions(Actions), print_actions(NewActions)]),
    {Nodes, State, NewActions};
process_action(Action, _, _, _) ->
    throw({unknown_action, Action}).

%%%% helpers

send_messages(Sender, Nodes, Messages) ->
    lists:foldl(
      fun(Message, Nds) ->
              case Message of
                  {unicast, Target, Msg} ->
                      #{Target := Nd = #node{queue = TargetQueue}} = Nds,
                      case Nd of
                          %% messages to running or partitioned nodes
                          %% are queued.
                          #node{status = RorP} when RorP == running;
                                                    RorP == partitioned ->
                              Nds#{Target => Nd#node{queue = TargetQueue ++ [{Sender, Msg}]}};
                          %% messages to downed nodes are dropped
                          #node{status = stopped} ->
                              Nds
                      end;
                  {multicast, Msg} ->
                      maps:map(fun(_ID, Nd = #node{queue = Q, status = RorP}) when RorP == running;
                                                                                   RorP == partitioned ->
                                       Nd#node{queue = Q ++ [{Sender, Msg}]};
                                  (_ID, Nd) ->
                                       Nd
                               end,
                               Nds);
                  Msg ->
                      throw({unhandled, Msg, Nds})
              end
      end,
      Nodes,
      Messages).

sched(round_robin, Current, Size, Start) ->
    ((Current + 1) rem Size) + Start;
sched(random, _Current, Size, Start) ->
    rand:uniform(Size) + (Start - 1).

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

process_timers(Time, Nodes) ->
    maps:map(fun(_ID, Node = #node{timer = undefined}) ->
                     Node;
                (_ID, Node = #node{timer = Deadline}) ->
                     case Time >= Deadline of
                         true ->
                             trace("timer expired for ~p", [_ID]),
                             Node#node{timer = undefined,
                                       queue = Node#node.queue ++ [{-1, timeout}]};
                         false ->
                             Node
                     end
             end,
             Nodes).


format_nodes(Nodes) ->
    format_nodes(maps:to_list(Nodes), []).

format_nodes([], Acc) ->
    lists:reverse(Acc);
format_nodes([{_, #node{name = Name,
                        status = Status,
                        queue = Queue,
                        state = _State}} | T],
              Acc) ->
    Display =
        io_lib:format("node ~p:~n"
                      "\t~p~n"
                      "\t~p~n",
                      [Name, Status, Queue]),
    format_nodes(T, [Display|Acc]).

seed_to_string({A, B, C}) ->
    integer_to_list(A) ++ "-" ++
        integer_to_list(B) ++ "-" ++
        integer_to_list(C).

file_close(FD) ->
    file:sync(FD),
    case file:close(FD) of
        ok -> ok;
        {error, _} ->
            ok = file:close(FD)
    end.

%% there likely needs to be some rewriting here to support arbitrary
%% protocol nesting?
print_message(timeout) ->
    <<"timeout">>;
print_message(Msg) ->
    case element(1, Msg) of
        Tag when is_atom(Tag) ->
            atom_to_binary(Tag, utf8);
        Sub when is_tuple(Sub) ->
            SubTag = element(1, element(2, Msg)),
            [io_lib:format("~p", [Sub]),
             $:,
             io_lib:format("~p", [SubTag])]
    end.

print_actions({result_and_send, _Result, {send, Messages}}) ->
    [<<"result+">>,[[print_action(Msg),$,] || Msg <- Messages]];
print_actions({send, Messages}) ->
    [[print_action(Msg),$,] || Msg <- Messages];
print_actions({send, _Mcast, Messages}) ->
    [[print_action(Msg),$,] || Msg <- Messages];
print_actions({result, _Result}) ->
    <<"output result">>;
print_actions([]) ->
    <<"ok">>;
print_actions(ok) ->
    <<"ok">>;
print_actions(ignore) ->
    <<"ignored">>;
print_actions(defer) ->
    <<"deferred">>;
print_actions(start_timer) ->
    <<"started timer">>.

print_action({unicast, Targ, Msg}) ->
    [print_message(Msg),"->",integer_to_binary(Targ)];
print_action({multicast, Msg}) ->
    [io_lib:format("~p", [element(1, Msg)]),<<"->all">>].
