-module(rc_state).

-export([
         is_rc_state/1,
         init/1,
         serialize/1,
         deserialize/2
        ]).

-record(rc_state,
        {
         prefix :: binary(),
         dirty_list :: [key()],
         state_map :: #{key() => any()},
         %% need to handle recursion here too
         ser_state_map :: #{key() => binary() | rc_state()},
         serializers :: #{key() => ser_fun()},
         deserializers :: #{key() => deser_fun()}
        }).

-type rc_state() :: #rc_state{}.
-type key() :: atom() | integer() | binary().
-type ser_fun() :: function().
-type deser_fun() :: function().

-spec is_rc_state(any()) -> boolean().
is_rc_state(#rc_state{}) ->
    true;
is_rc_state(_) ->
    false.

-spec init([{key(), ser_fun(), deser_fun()}]) -> rc_state().
init(Prefix, SpecList) ->
    parse_specs(SpecList, #rc_state{prefix = Prefix}).

parse_specs([{}

-spec serialize(rc_state()) -> binary().
seralize(_) -> %#rc_state{dirt}) ->
    <<>>.
