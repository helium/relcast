# How to write a fakecast test

## Init and Inputs

When using randomness in the initial state or in inputs, it's important to define them as a thunk (0-arity function) so that they're executed after the random seed has been set.  Otherwise you won't get full determinism, as the seed for generating the input or init state will be different than that of the test.

## Running tests

For the most part I've been embedding these tests within common test, and then using the shell to run them more than once, to look for

## TODO

- adding no-op crypto to some of the protocols so we can run iterations more quickly to explore more state space.
- add more actions so we can explore more complex cases.
- write some more complicated tests to get more experience with complex models and how we might want to change the code.

## Models

Writing models is currently quite complex, and hopefully as we get more experience with using them, we'll have some useful refactorings to try that will make writing models simpler and more powerful.

The current signature of a model function is:
```

-spec model(Message :: message(), 
            From :: pos_integer(),
            To :: pos_integer(),
            NodeState :: term(),
            NewState :: term(),
            Actions :: [term()],
            ModelState :: term()) -> model_output().

Model(Message, From, Node, NodeState, NewState, Actions, TestState)
```

The model is processed after a node has processed the message, hence we have `NodeState`, the state before the message, and `NewState`, the state from afterwards.  The `Message` is the message that was processed.  `From` and `To` are the node indices of the sender and receiver respectively.  `Actions` were the actions returned by with `NewState`, and `ModelState` is the persistent state of the model.

A trivial model which just accumulates results from a single round and otherwise changes nothing, follows:
```
trivial(_Message, _From, To, _NodeState, _NewState, {result, Result},
        #state{results = Results0} = State) ->
    Results = sets:add_element({result, {To, Result}}, Results0),
    case sets:size(Results) == State#state.node_count of
        true ->
            {result, Results};
        false ->
            {actions, [], State#state{results = Results}}
    end;
trivial(_Message, _From, _To, _NodeState, _NewState, _Actions, ModelState) ->
    {actions, [], ModelState}.
```

For now, if you want to encode invariants over the state, you must encode them directly.  In the future we should consider adding a new phase for invariant checking (to make it easier to check invariants across all model states), if this proves to be clunky.






### Cookbook

Here we should accumulate various idioms, since dropping a message, or partitioning two nodes, or etc may not be immediately clear.


#### Partition two nodes

```
model(_, 1, 2, NodeState, _NewState, _Actions, ModelState) ->
    {actions, [{alter_state, NodeState},
               {alter_actions, ok}], ModelState};
```

#### Alter an outgoing message

```
model(_Msg, _from, 1, _NodeState, _NewState, {send,[{multicast,Send}]},
      ModelState) when element(1, Send) == send ->
    NewActions = [{unicast, N, Send} || N <- [2,3,4,5]],
    {actions, [{alter_actions, {send, NewActions}}],
     ModelState};
```

#### Drop a message
```
model(Send, _from, 1, _NodeState, _NewState, _Actions,
      ModelState) when element(1, Send) =:= send ->
    {actions, [{alter_state, NodeState}, {alter_actions, ok}],
     ModelState};
```

