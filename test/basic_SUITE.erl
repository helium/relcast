-module(basic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0
]).

-export([
    basic/1,
    stop_resume/1
]).

all() ->
    [basic, stop_resume].

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
