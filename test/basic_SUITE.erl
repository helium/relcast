-module(basic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0
]).

-export([
    basic/1
]).

all() ->
    [basic].

basic(_Config) ->
    Actors = lists:seq(1, 3),
    {ok, RC1} = relcast:start(1, Actors, test_handler, [{data_dir, "data"}]),
    ct:pal("RC1: ~p", [RC1]),
    {ok, RC2} = relcast:start(2, Actors, test_handler, [{data_dir, "data"}]),
    ct:pal("RC2: ~p", [RC2]),
    {ok, RC3} = relcast:start(3, Actors, test_handler, [{data_dir, "data"}]),
    ct:pal("RC3: ~p", [RC3]),
    ok.
