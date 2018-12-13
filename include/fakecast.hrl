-type ordering() :: round_robin | random. %% others?  attack_leader?  nemesis?
-type strategy() :: favor_sequential | favor_concurrent.  %% time
-type init_state() :: started | stopped.

-type name() :: atom().
-type node_id() :: pos_integer().

-record(fc_conf,
        {
         test_mod :: atom(),
         model :: function(),
         ordering = random :: ordering(),
         strategy = favor_concurrent :: strategy(),
         nodes :: [node_id()],
         configs :: [{name(), init_state(), term()}],
         id_start = 0 :: 0 | 1,
         max_time = 1500 :: pos_integer()
        }).
