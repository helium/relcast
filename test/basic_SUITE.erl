-module(basic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0
]).

-export([
    basic/1,
    stop_resume/1,
    defer/1,
    defer_stop_resume/1
]).

all() ->
    [basic, stop_resume, defer, defer_stop_resume].

basic(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data1"}]),
    not_found = relcast:take(2, RC1),
    {false, _} = relcast:command(is_done, RC1),
    {ok, RC1_2} = relcast:deliver(<<"hello">>, 2, RC1),
    {true, _} = relcast:command(is_done, RC1_2),
    {ok, Ref, <<"ehlo">>, RC1_3} = relcast:take(2, RC1_2),
    %% same result if we take() again for this actor
    {ok, Ref, <<"ehlo">>, RC1_3} = relcast:take(2, RC1_3),
    %% ack it
    {ok, RC1_4} = relcast:ack(2, Ref, RC1_3),
    %% check it's gone
    {ok, Ref2, <<"hai">>, RC1_5} = relcast:take(2, RC1_4),
    %% take for actor 3
    {ok, Ref3, <<"hai">>, RC1_6} = relcast:take(3, RC1_5),
    %% ack with the wrong ref
    {ok, RC1_7} = relcast:ack(3, Ref, RC1_6),
    %% actor 3 still has pending data
    {ok, Ref3, <<"hai">>, RC1_8} = relcast:take(3, RC1_7),
    %% ack both of the outstanding messages
    {ok, RC1_9} = relcast:ack(2, Ref2, RC1_8),
    not_found = relcast:take(2, RC1_9),
    {ok, RC1_10} = relcast:ack(3, Ref3, RC1_9),
    not_found = relcast:take(2, RC1_10),
    relcast:stop(normal, RC1_10),
    ok.

stop_resume(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2"}]),
    not_found = relcast:take(2, RC1),
    {false, _} = relcast:command(is_done, RC1),
    {ok, RC1_2} = relcast:deliver(<<"hello">>, 2, RC1),
    {true, _} = relcast:command(is_done, RC1_2),
    {ok, Ref, <<"ehlo">>, RC1_3} = relcast:take(2, RC1_2),
    %% same result if we take() again for this actor
    {ok, Ref, <<"ehlo">>, RC1_3} = relcast:take(2, RC1_3),
    %% ack it
    {ok, RC1_3a} = relcast:ack(2, Ref, RC1_3),
    relcast:stop(normal, RC1_3a),
    {ok, RC1_4} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2"}]),
    %% check it's gone
    {ok, Ref2, <<"hai">>, RC1_5} = relcast:take(2, RC1_4),
    %% take for actor 3
    {ok, Ref3, <<"hai">>, RC1_6} = relcast:take(3, RC1_5),
    %% ack with the wrong ref
    {ok, RC1_7} = relcast:ack(3, Ref, RC1_6),
    %% actor 3 still has pending data
    {ok, Ref3, <<"hai">>, RC1_7a} = relcast:take(3, RC1_7),
    relcast:stop(normal, RC1_7a),
    {ok, RC1_8} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2"}]),
    %% ack both of the outstanding messages
    {ok, RC1_9} = relcast:ack(2, Ref2, RC1_8),
    %% we lost the ack, so the message is still in-queue
    {ok, Ref4, <<"hai">>, RC1_10} = relcast:take(2, RC1_9),
    {ok, RC1_11} = relcast:ack(3, Ref3, RC1_10),
    {ok, Ref5, <<"hai">>, RC1_12} = relcast:take(2, RC1_11),
    %% ack both of the outstanding messages again
    {ok, RC1_13} = relcast:ack(2, Ref4, RC1_12),
    not_found = relcast:take(2, RC1_13),
    {ok, RC1_14} = relcast:ack(3, Ref5, RC1_13),
    not_found = relcast:take(2, RC1_14),
    relcast:stop(normal, RC1_14),
    ok.

defer(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data3"}]),
    %% try to put an entry in the seq map, it will be deferred because the
    %% relcast is in round 0
    {defer, RC1_2} = relcast:deliver(<<"seq", 1:8/integer>>, 2, RC1),
    {0, _} = relcast:command(round, RC1_2),
    {#{}, _} = relcast:command(seqmap, RC1_2),
    {ok, RC1_3} = relcast:command(next_round, RC1_2),
    {Map, _} = relcast:command(seqmap, RC1_3),
    [{2, 1}] = maps:to_list(Map),
    %% queue up several seq messages. all are valid but only the first can apply
    %% right now
    {ok, RC1_4} = relcast:deliver(<<"seq", 1:8/integer>>, 3, RC1_3),
    {defer, RC1_5} = relcast:deliver(<<"seq", 2:8/integer>>, 3, RC1_4),
    {defer, RC1_5a} = relcast:deliver(<<"seq", 3:8/integer>>, 3, RC1_5),
    %% also attempt to queue a sequence number for 2 with a break, so it's not
    %% valid - this should not be deferred
    {ok, RC1_6} = relcast:deliver(<<"seq", 3:8/integer>>, 2, RC1_5a),
    {Map2, _} = relcast:command(seqmap, RC1_6),
    [{2, 1}, {3, 1}] = lists:sort(maps:to_list(Map2)),
    %% increment the round, the seqmap should increment for 3
    {ok, RC1_7} = relcast:command(next_round, RC1_6),
    {Map3, _} = relcast:command(seqmap, RC1_7),
    [{2, 1}, {3, 2}] = lists:sort(maps:to_list(Map3)),
    %% increment it again, it should increment seqmap for 3 again
    {ok, RC1_8} = relcast:command(next_round, RC1_7),
    {Map4, _} = relcast:command(seqmap, RC1_8),
    [{2, 1}, {3, 3}] = lists:sort(maps:to_list(Map4)),
    relcast:stop(normal, RC1_8),
    ok.

defer_stop_resume(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data4"}]),
    %% try to put an entry in the seq map, it will be deferred because the
    %% relcast is in round 0
    {defer, RC1_2} = relcast:deliver(<<"seq", 1:8/integer>>, 2, RC1),
    {0, _} = relcast:command(round, RC1_2),
    {#{}, _} = relcast:command(seqmap, RC1_2),
    {ok, RC1_3} = relcast:command(next_round, RC1_2),
    {Map, _} = relcast:command(seqmap, RC1_3),
    [{2, 1}] = maps:to_list(Map),
    %% queue up several seq messages. all are valid but only the first can apply
    %% right now
    {ok, RC1_4} = relcast:deliver(<<"seq", 1:8/integer>>, 3, RC1_3),
    {defer, RC1_5} = relcast:deliver(<<"seq", 2:8/integer>>, 3, RC1_4),
    {defer, RC1_5a} = relcast:deliver(<<"seq", 3:8/integer>>, 3, RC1_5),
    %% stop and resume the relcast here to make sure we recover all our states
    %% correctly
    relcast:stop(normal, RC1_5a),
    {ok, RC1_5b} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data4"}]),
    %% also attempt to queue a sequence number for 2 with a break, so it's not
    %% valid - this should not be deferred
    {ok, RC1_6} = relcast:deliver(<<"seq", 3:8/integer>>, 2, RC1_5b),
    {Map2, _} = relcast:command(seqmap, RC1_6),
    [{2, 1}, {3, 1}] = lists:sort(maps:to_list(Map2)),
    %% increment the round, the seqmap should increment for 3
    {ok, RC1_7} = relcast:command(next_round, RC1_6),
    {Map3, _} = relcast:command(seqmap, RC1_7),
    [{2, 1}, {3, 2}] = lists:sort(maps:to_list(Map3)),
    %% increment it again, it should increment seqmap for 3 again
    {ok, RC1_8} = relcast:command(next_round, RC1_7),
    {Map4, _} = relcast:command(seqmap, RC1_8),
    [{2, 1}, {3, 3}] = lists:sort(maps:to_list(Map4)),
    relcast:stop(normal, RC1_8),
    ok.
