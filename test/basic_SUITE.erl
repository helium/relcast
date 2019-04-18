-module(basic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
         all/0,
         init_per_suite/1,
         end_per_suite/1
        ]).

-export([
         basic/1,
         basic2/1,
         stop_resume/1,
         upgrade_stop_resume/1,
         defer/1,
         defer_stop_resume/1,
         epochs/1,
         epochs_gc/1,
         callback_message/1,
         self_callback_message/1,
         pipeline/1,
         write_reduction/1,
         state_split/1
        ]).

all() ->
    [
     basic,
     basic2,
     stop_resume,
     upgrade_stop_resume,
     defer,
     defer_stop_resume,
     epochs,
     epochs_gc,
     callback_message,
     self_callback_message,
     pipeline,
     %%write_reduction  %% this isn't really baked enough, just call manually
     state_split
    ].

init_per_suite(Config) ->
    application:load(relcast),
    application:set_env(relcast, defer_count_threshold, -1),
    application:set_env(relcast, defer_time_threshold, -1),
    Config.

end_per_suite(Config) ->
    Config.

basic(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data1"}]),
    {not_found, _, _} = relcast:take(2, RC1),
    {false, _} = relcast:command(is_done, RC1),
    {ok, RC1_2} = relcast:deliver(1, <<"hello">>, 2, RC1),
    {true, _} = relcast:command(is_done, RC1_2),
    {ok, Seq, [1], <<"ehlo">>, RC1_3} = relcast:take(2, RC1_2),
    %% ack it
    {ok, RC1_4} = relcast:ack(2, Seq, RC1_3),
    %% check it's gone
    {ok, Seq2, [], <<"hai">>, RC1_5} = relcast:take(2, RC1_4),
    %% take for actor 3
    {ok, _Seq3, [], <<"hai">>, RC1_6} = relcast:take(3, RC1_5),
    %% ack with the wrong ref
    {ok, RC1_7} = relcast:ack(3, Seq, RC1_6),
    %% actor 3 still has pending data, but we need to clear pending to
    %% get at it
    {ok, Seq3_1, [], <<"hai">>, RC1_8} = relcast:take(3, RC1_7, true),
    %% ack both of the outstanding messages
    {ok, RC1_9} = relcast:ack(2, Seq2, RC1_8),
    {not_found, _, _} = relcast:take(2, RC1_9),
    {ok, RC1_10} = relcast:ack(3, Seq3_1, RC1_9),
    {not_found, _, _} = relcast:take(2, RC1_10),
    relcast:stop(normal, RC1_10),
    ok.

basic2(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1a} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data13a"}]),
    {ok, RC1b} = relcast:start(2, Actors, test_handler, [2], [{data_dir, "data13b"}]),
    {not_found, _, _} = relcast:take(2, RC1a),
    {not_found, _, _} = relcast:take(1, RC1b),
    {ok, RC2a} = relcast:command({init, 2}, RC1a),
    {ok, RC2b} = relcast:command({init, 1}, RC1b),

    {ok, SeqA, [], MsgA, RC3a} = relcast:take(2, RC2a),
    {ok, SeqB, [], MsgB, RC3b} = relcast:take(1, RC2b),

    ct:pal("seqs a ~p ~p b ~p ~p", [SeqA, MsgA, SeqB, MsgB]),

    {ok, RC3a1} = relcast:deliver(SeqB, MsgB, 2, RC3a),
    {ok, RC3b1} = relcast:deliver(SeqA, MsgA, 1, RC3b),
    {ok, RC4a} = relcast:ack(2, SeqA, RC3a1),
    {ok, RC4b} = relcast:ack(1, SeqB, RC3b1),

    {ok, SeqA1, [SeqB], <<"ehlo">>, RC5a} = relcast:take(2, RC4a),
    {ok, SeqB1, [SeqA], <<"ehlo">>, RC5b} = relcast:take(1, RC4b),

    {true, _} = relcast:command(is_done, RC5a),
    {true, _} = relcast:command(is_done, RC5b),

    %% ack it
    {ok, RC6a} = relcast:ack(2, SeqA1, RC5a),
    {ok, RC6b} = relcast:ack(1, SeqB1, RC5b),
    %% check it's gone
    {ok, SeqA2, [], <<"hai">>, RC7a} = relcast:take(2, RC6a),
    {ok, SeqB2, [], <<"hai">>, RC7b} = relcast:take(1, RC6b, true),

    {ok, RC8a} = relcast:ack(2, SeqA2, RC7a),
    {ok, RC8b} = relcast:ack(2, SeqB2, RC7b),

    {not_found, _, _} = relcast:take(2, RC8a),
    {not_found, _, _} = relcast:take(1, RC8b),

    relcast:stop(normal, RC8a),
    relcast:stop(normal, RC8b),
    ok.

stop_resume(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2"}]),
    {not_found, _, _} = relcast:take(2, RC1),
    {false, _} = relcast:command(is_done, RC1),
    {ok, RC1_2} = relcast:deliver(1, <<"hello">>, 2, RC1),
    {true, _} = relcast:command(is_done, RC1_2),
    {_, [], Outbound} = relcast:status(RC1_2),
    [<<"ehlo">>, <<"hai">>] = maps:get(2, Outbound),
    [<<"hai">>] = maps:get(3, Outbound),
    {ok, Ref, [1], <<"ehlo">>, RC1_3} = relcast:take(2, RC1_2),
    %% ack it
    {ok, RC1_3a} = relcast:ack(2, Ref, RC1_3),
    relcast:stop(normal, RC1_3a),
    {ok, RC1_4} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2"}]),
    %% check it's gone
    {ok, Ref2, _, <<"hai">>, RC1_5} = relcast:take(2, RC1_4),
    %% take for actor 3
    {ok, _Ref3, _, <<"hai">>, RC1_6} = relcast:take(3, RC1_5),
    %% ack with the wrong ref
    {ok, RC1_7} = relcast:ack(3, Ref, RC1_6),
    %% actor 3 still has pending data
    {ok, Ref3, _, <<"hai">>, RC1_7a} = relcast:take(3, RC1_7, true),
    relcast:stop(normal, RC1_7a),
    {ok, RC1_8} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2"}]),
    %% ack both of the outstanding messages
    {ok, RC1_9} = relcast:ack(2, Ref2, RC1_8),
    %% we lost the ack, so the message is still in-queue
    {ok, Ref4, _, <<"hai">>, RC1_10} = relcast:take(2, RC1_9),
    {ok, RC1_11} = relcast:ack(3, Ref3, RC1_10),
    {ok, Ref5, _, <<"hai">>, RC1_12} = relcast:take(2, RC1_11, true),
    %% ack both of the outstanding messages again
    {ok, RC1_13} = relcast:ack(2, Ref4, RC1_12),
    {not_found, _, _} = relcast:take(2, RC1_13),
    {ok, RC1_14} = relcast:ack(3, Ref5, RC1_13),
    {not_found, _, _} = relcast:take(2, RC1_14),
    relcast:stop(normal, RC1_14),
    ok.

-define(stored_module_state, <<"stored_module_state">>).

upgrade_stop_resume(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2a"}]),
    {not_found, _, _} = relcast:take(2, RC1),
    {false, _} = relcast:command(is_done, RC1),
    {ok, RC1_2} = relcast:deliver(1, <<"hello">>, 2, RC1),
    {true, _} = relcast:command(is_done, RC1_2),
    {_, [], Outbound} = relcast:status(RC1_2),
    [<<"ehlo">>, <<"hai">>] = maps:get(2, Outbound),
    [<<"hai">>] = maps:get(3, Outbound),
    {ok, Ref, _, <<"ehlo">>, RC1_3} = relcast:take(2, RC1_2),
    %% ack it
    {ok, RC1_3a} = relcast:ack(2, Ref, RC1_3),
    relcast:stop(normal, RC1_3a),

    %% now that we've stopped, perform some surgery on the underlying rocks
    {ok, DB, [_, CF, _]} = rocksdb:open_with_cf("data2a", [{create_if_missing, true}],
                                                [{"default", []},
                                                 {"Inbound", []},
                                                 {"epoch0000000000", []}]),
    %% get the data from the default
    {ok, SerializedModuleState} = rocksdb:get(DB, ?stored_module_state, []),
    %% write to old CF
    ok = rocksdb:put(DB, CF, ?stored_module_state, SerializedModuleState, []),
    {ok, SerializedModuleState} = rocksdb:get(DB, CF, ?stored_module_state, []),
    %% delete from default CF
    ok = rocksdb:delete(DB, ?stored_module_state, []),
    %% close
    rocksdb:close(DB),

    %%proceed as planned

    {ok, RC1_4} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2a"}]),
    %% check it's gone
    {ok, Ref2, _, <<"hai">>, RC1_5} = relcast:take(2, RC1_4),
    %% take for actor 3
    {ok, _Ref3, _, <<"hai">>, RC1_6} = relcast:take(3, RC1_5),
    %% ack with the wrong ref
    {ok, RC1_7} = relcast:ack(3, Ref, RC1_6),
    %% actor 3 still has pending data
    {ok, Ref3, _, <<"hai">>, RC1_7a} = relcast:take(3, RC1_7, true),
    relcast:stop(normal, RC1_7a),
    {ok, RC1_8} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data2a"}]),
    %% ack both of the outstanding messages
    {ok, RC1_9} = relcast:ack(2, Ref2, RC1_8),
    %% we lost the ack, so the message is still in-queue
    {ok, Ref4, _, <<"hai">>, RC1_10} = relcast:take(2, RC1_9),
    {ok, RC1_11} = relcast:ack(3, Ref3, RC1_10),
    {ok, Ref5, _, <<"hai">>, RC1_12} = relcast:take(2, RC1_11, true),
    %% ack both of the outstanding messages again
    {ok, RC1_13} = relcast:ack(2, Ref4, RC1_12),
    {not_found, _, _} = relcast:take(2, RC1_13),
    {ok, RC1_14} = relcast:ack(3, Ref5, RC1_13),
    {not_found, _, _} = relcast:take(2, RC1_14),
    relcast:stop(normal, RC1_14),
    ok.

defer(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data3"}]),
    %% try to put an entry in the seq map, it will be deferred because the
    %% relcast is in round 0
    {ok, RC1_2} = relcast:deliver(1, <<"seq", 1:8/integer>>, 2, RC1),
    {_, [{2, <<"seq", 1:8/integer>>}], Outbound} = relcast:status(RC1_2),
    0 = maps:size(Outbound),
    {0, _} = relcast:command(round, RC1_2),
    {#{}, _} = relcast:command(seqmap, RC1_2),
    {ok, RC1_3} = relcast:command(next_round, RC1_2),
    {Map, _} = relcast:command(seqmap, RC1_3),
    [{2, 1}] = maps:to_list(Map),
    %% queue up several seq messages. all are valid but only the first can apply
    %% right now
    {ok, RC1_4} = relcast:deliver(2, <<"seq", 1:8/integer>>, 3, RC1_3),
    {ok, RC1_5} = relcast:deliver(3, <<"seq", 2:8/integer>>, 3, RC1_4),
    {ok, RC1_5a} = relcast:deliver(4 ,<<"seq", 3:8/integer>>, 3, RC1_5),
    %% also attempt to queue a sequence number for 2 with a break, so it's not
    %% valid - this should not be deferred
    {ok, RC1_6} = relcast:deliver(5, <<"seq", 3:8/integer>>, 2, RC1_5a),
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
    {ok, RC1_2} = relcast:deliver(1, <<"seq", 1:8/integer>>, 2, RC1),
    {0, _} = relcast:command(round, RC1_2),
    {#{}, _} = relcast:command(seqmap, RC1_2),
    {ok, RC1_3} = relcast:command(next_round, RC1_2),
    {Map, _} = relcast:command(seqmap, RC1_3),
    [{2, 1}] = maps:to_list(Map),
    %% queue up several seq messages. all are valid but only the first can apply
    %% right now
    {ok, RC1_4} = relcast:deliver(2, <<"seq", 1:8/integer>>, 3, RC1_3),
    {ok, RC1_5} = relcast:deliver(3, <<"seq", 2:8/integer>>, 3, RC1_4),
    {ok, RC1_5a} = relcast:deliver(4, <<"seq", 3:8/integer>>, 3, RC1_5),
    %% stop and resume the relcast here to make sure we recover all our states
    %% correctly
    relcast:stop(normal, RC1_5a),
    {ok, RC1_5b} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data4"}]),
    %% also attempt to queue a sequence number for 2 with a break, so it's not
    %% valid - this should not be deferred
    {ok, RC1_6} = relcast:deliver(5, <<"seq", 3:8/integer>>, 2, RC1_5b),
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

epochs(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data5"}]),
    {ok, RC1_1a} = relcast:deliver(1, <<"hello">>, 2, RC1),
    %% try to put an entry in the seq map, it will be deferred because the
    %% relcast is in round 0
    {ok, RC1_2} = relcast:deliver(2, <<"seq", 1:8/integer>>, 2, RC1_1a),
    {0, _} = relcast:command(round, RC1_2),
    %% go to the next epoch
    {ok, RC1_3} = relcast:command(next_epoch, RC1_2),
    %% queue up another deferred message
    {ok, RC1_4} = relcast:deliver(3, <<"seq", 2:8/integer>>, 2, RC1_3),
    %% go to the next round
    {ok, RC1_5} = relcast:command(next_round, RC1_4),
    %% check the deferred message from the previous epoch gets handled
    {Map, _} = relcast:command(seqmap, RC1_5),
    [{2, 1}] = maps:to_list(Map),
    %% go to the next round
    {ok, RC1_6} = relcast:command(next_round, RC1_5),
    {2, _} = relcast:command(round, RC1_6),
    %% check the deferred message from the current epoch gets handled
    {Map2, _} = relcast:command(seqmap, RC1_6),
    [{2, 2}] = maps:to_list(Map2),
    %% GC'd outbound data queued from the first epoch
    {not_found, _, RC1_7} = relcast:take(2, RC1_6),
    relcast:stop(normal, RC1_7),
    {ok, CFs} = rocksdb:list_column_families("data5", []),
    ["default", "Inbound", "epoch0000000001"] = CFs,
    %% reopen the relcast and make sure it didn't delete any wrong column
    %% families
    {ok, RC1_10} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data6"}]),
    relcast:stop(normal, RC1_10),
    {ok, CFs} = rocksdb:list_column_families("data5", []),
    ok.

epochs_gc(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data6"}]),
    %% try to put an entry in the seq map, it will be deferred because the
    %% relcast is in round 0
    {ok, RC1_1a} = relcast:deliver(1, <<"hello">>, 2, RC1),
    {ok, RC1_2} = relcast:deliver(2, <<"seq", 1:8/integer>>, 2, RC1_1a),
    {0, _} = relcast:command(round, RC1_2),
    %% go to the next epoch
    {ok, RC1_3} = relcast:command(next_epoch, RC1_2),
    %% go to the next epoch, this should GC the original epoch
    {ok, RC1_4} = relcast:command(next_epoch, RC1_3),
    %% this is now invalid because we're lacking the previous one
    {ok, RC1_5} = relcast:deliver(3, <<"seq", 2:8/integer>>, 2, RC1_4),
    %% go to the next round
    {ok, RC1_6} = relcast:command(next_round, RC1_5),
    %% check the deferred message from the previous epoch gets handled
    {Map, _} = relcast:command(seqmap, RC1_6),
    [{2, 1}] = maps:to_list(Map),
    %% go to the next round
    {ok, RC1_7} = relcast:command(next_round, RC1_6),
    {2, _} = relcast:command(round, RC1_7),
    %% check the deferred message from the previous epoch gets handled
    {Map2, _} = relcast:command(seqmap, RC1_7),
    [{2, 2}] = maps:to_list(Map2),
    %% the data from the original epoch has been GC'd
    {not_found, _, _} = relcast:take(2, RC1_7),
    relcast:stop(normal, RC1_7),
    {ok, CFs} = rocksdb:list_column_families("data6", []),
    ["default", "Inbound", "epoch0000000002"] = CFs,
    %% reopen the relcast and make sure it didn't delete any wrong column
    %% families
    {ok, RC1_10} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data6"}]),
    relcast:stop(normal, RC1_10),
    {ok, CFs} = rocksdb:list_column_families("data6", []),
    %% Re-add the old epoch 0 CF and check it gets removed
    {ok, DB, _} = rocksdb:open_with_cf("data6", [{create_if_missing, true}],
                                       [{"default", []},
                                        {"Inbound", []},
                                        {"epoch0000000002", []}]),
    {ok, _Epoch0} = rocksdb:create_column_family(DB, "epoch0000000000", []),
    rocksdb:close(DB),
    {ok, RC1_11} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data6"}]),
    relcast:stop(normal, RC1_11),
    {ok, ["default", "Inbound", "epoch0000000002"]} = rocksdb:list_column_families("data6", []),
    ok.

callback_message(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data7"}]),
    {ok, RC1_2} = relcast:deliver(1, <<"greet">>, 2, RC1),
    {ok, Ref, _, <<"greetings to 2">>, RC1_3} = relcast:take(2, RC1_2),
    {ok, Ref2, _, <<"greetings to 3">>, RC1_4} = relcast:take(3, RC1_3),
    {ok, RC1_5} = relcast:ack(2, Ref, RC1_4),
    {ok, RC1_6} = relcast:ack(3, Ref2, RC1_5),
    {not_found, _, RC1_7} = relcast:take(2, RC1_6),
    {not_found, _, RC1_8} = relcast:take(3, RC1_7),
    relcast:stop(normal, RC1_8),
    ok.

self_callback_message(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data8"}]),
    {false, _} = relcast:command(was_saluted, RC1),
    {ok, RC1_2} = relcast:deliver(1, <<"salute">>, 2, RC1),
    {ok, Ref,  _, <<"salutations to 2">>, RC1_3} = relcast:take(2, RC1_2),
    %%{ok, Ref, _, <<"salutations to 2">>, _} = relcast:take(2, RC1_3),
    {ok, Ref2, _, <<"salutations to 3">>, RC1_4} = relcast:take(3, RC1_3),
    {ok, RC1_5} = relcast:ack(2, Ref, RC1_4),
    {ok, RC1_6} = relcast:ack(3, Ref2, RC1_5),
    {not_found, _, RC1_7} = relcast:take(2, RC1_6),
    {not_found, _, RC1_8} = relcast:take(3, RC1_7),
    {true, _} = relcast:command(was_saluted, RC1_8),
    relcast:stop(normal, RC1_8),
    ok.

pipeline(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data11"}]),
    {not_found, _, _} = relcast:take(2, RC1),
    {ok, RC2} =
        lists:foldl(fun(Idx, {ok, Acc}) ->
                            relcast:deliver(Idx, <<"unicast: hello - ", (integer_to_binary(Idx))/binary>>, 2, Acc)
                    end,
                    {ok, RC1},
                    [N || N <- lists:seq(1, 30)]),
    {ok, Ref1, _, <<"hello - 18">>, RC3} =
        lists:foldl(fun(_Idx, {ok, _R, _Acks, _Msg, Acc}) ->
                            relcast:take(2, Acc)
                    end,
                    {ok, ign, ign, ign, RC2},
                    [N || N <- lists:seq(1, 18)]),
    {ok, Ref2, _, <<"hello - 19">>, RC4} = relcast:take(2, RC3),
    {ok, Ref3, _, <<"hello - 20">>, RC5} = relcast:take(2, RC4),
    {pipeline_full, _, _RC4} = relcast:take(2, RC5),
    20 = relcast:in_flight(2, RC5),
    %% singly ack the second-to-last message first
    {ok, RC6} = relcast:ack(2, Ref2, RC5),
    19 = relcast:in_flight(2, RC6),
    %% test multi-ack
    {ok, RC7} = relcast:ack(2, lists:seq(Ref1 - 17, Ref1), RC6),
    1 = relcast:in_flight(2, RC7),
    %% ack the last message singly
    {ok, RC8} = relcast:ack(2, Ref3, RC7),
    0 = relcast:in_flight(2, RC8),
    %% test single acks
    RC9 =
        lists:foldl(fun(Idx, Acc) ->
                            Msg = <<"hello - ", (integer_to_binary(Idx))/binary>>,
                            0 = relcast:in_flight(2, Acc),
                            {ok, R, _, Msg, Acc1} = relcast:take(2, Acc),
                            1 = relcast:in_flight(2, Acc1),
                            {ok, Acc2} = relcast:ack(2, R, Acc1),
                            0 = relcast:in_flight(2, Acc2),
                            Acc2
                    end,
                    RC8,
                    [N || N <- lists:seq(21, 30)]),
    {not_found, _, _} = relcast:take(2, RC9),
    ok.

state_split(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {ok, _} = relcast:command(populate, RC1),
    %% normal stop and start
    relcast:stop(normal, RC1),
    {ok, RC2} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State, _} = relcast:command(get, RC2),
    ?assertEqual({state, a, b, c, d, e}, State),
    relcast:stop(normal, RC2),

    ct:pal("stopped"),

    %%ct:pal("~s", [os:cmd("pwd")]),
    %%ct:pal("~s", [os:cmd("ls ../../../../test/")]),

    true = code:add_patha("."),
    [] = os:cmd("cp ../../../../test/handler* ."),

    [] = os:cmd("mv handler1-a.er handler1.erl"),
    [] = os:cmd("erlc -DCOMMON_TEST -DTEST handler1.erl; sync; sync"),

    true = code:delete(handler1),
    false = code:purge(handler1),
    {module, handler1} = code:load_abs("handler1"),
    [] = os:cmd("rm handler1.beam"),

    ct:pal("loaded a"),

    {ok, RC3} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State2, _} = relcast:command(get, RC3),
    ?assertEqual({state, a, b, c, d, e}, State2),
    {ok, RC4} = relcast:command(populate2, RC3),
    relcast:stop(normal, RC4),

    {ok, RC3a} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State2a, _} = relcast:command(get, RC3a),
    ?assertEqual({state, a1, b1, c1, d1, e1}, State2a),
    relcast:stop(normal, RC3a),

    %% upgrade to a bigger state tuple

    [] = os:cmd("mv handler1-b.er handler1.erl"),
    [] = os:cmd("erlc -DCOMMON_TEST -DTEST handler1.erl; sync; sync"),

    true = code:delete(handler1),
    false = code:purge(handler1),
    {module, handler1} = code:load_abs("handler1"),
    [] = os:cmd("rm handler1.beam"),

    ct:pal("loaded b"),

    {ok, RC5} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State3, _} = relcast:command(get, RC5),

    ?assertEqual({state, a1, b1, c1, d1, e1, undefined, undefined}, State3),
    {ok, RC6} = relcast:command(populate3, RC5),
    relcast:stop(normal, RC6),

    {ok, RC5a} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State3a, _} = relcast:command(get, RC5a),
    ?assertEqual({state, a2, b2, c2, d2, e2, f2, g2}, State3a),
    relcast:stop(normal, RC5a),

    %% add a sub-behavior

    [] = os:cmd("mv handler1-c.er handler1.erl"),
    [] = os:cmd("erlc -DCOMMON_TEST -DTEST handler1.erl; sync; sync"),

    true = code:delete(handler1),
    false = code:purge(handler1),
    {module, handler1} = code:load_abs("handler1"),
    [] = os:cmd("rm handler1.beam"),

    ct:pal("loaded c"),

    {ok, RC7} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State4, _} = relcast:command(get, RC7),

    ?assertEqual({state, a2, b2, c2, {dstate, d2, d2, d2}, e2, f2, g2}, State4),
    {ok, RC8} = relcast:command(populate4, RC7),
    relcast:stop(normal, RC8),

    {ok, RC7a} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State4a, _} = relcast:command(get, RC7a),

    ?assertEqual({state, a3, b3, c3, {dstate, d3a, d3b, d3c}, e3, f3, g3}, State4a),
    relcast:stop(normal, RC7a),

    %% downgrade back to binary

    [] = os:cmd("mv handler1-d.er handler1.erl"),
    [] = os:cmd("erlc -DCOMMON_TEST -DTEST handler1.erl; sync; sync"),

    true = code:delete(handler1),
    false = code:purge(handler1),
    {module, handler1} = code:load_abs("handler1"),
    [] = os:cmd("rm handler1.beam"),

    ct:pal("loaded d"),

    {ok, RC9} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State5, _} = relcast:command(get, RC9),

    ?assertEqual({state, a3, b3, c3, {dstate, d3a, d3b, d3c}, e3, f3, g3}, State5),
    {ok, RC10} = relcast:command(populate5, RC9),
    relcast:stop(normal, RC10),

    {ok, RC9a} = relcast:start(1, Actors, handler1, [], [{data_dir, "data12"}]),
    {State5a, _} = relcast:command(get, RC9a),

    ?assertEqual({state, a4, b4, c4, {dstate, d4a, d4b, d4c}, e4, f4, g4}, State5a),
    relcast:stop(normal, RC9a),

    ok.


write_reduction(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [1], [{data_dir, "data12"}]),

    {ok, RC2} =
        lists:foldl(fun(Idx, {ok, Acc}) ->
                            {ok, Acc1} = relcast:deliver(<<"unicast: hello - ",
                                                           (integer_to_binary(Idx))/binary>>, 2, Acc),
                            {ok, R, _Msg, Acc2} = relcast:take(2, Acc1),
                            relcast:ack(2, R, Acc2)
                    end,
                    {ok, RC1},
                    [N || N <- lists:seq(1, 10000)]),

    RCF = RC2,
    DB = element(2, RCF),
    Stats =
        [begin
             {ok, S} = rocksdb:stats(DB, CF),
             io:fwrite(standard_error, "~s~n", [S])
         end
         || CF <- [get_active(RCF), get_inbound(RCF)]],
    ?assertEqual(ok, Stats),
    ok.

get_active(S) ->
    element(13, S).

get_inbound(S) ->
    element(12, S).
