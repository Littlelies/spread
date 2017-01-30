-module(spread_topic_propagator).

-export([propagate/1]).

-spec propagate(spread_event:event()) -> [any()].
propagate(Event) ->
    Locals = propagate_locally(Event),
    Outsides = propagate_outside(Event),
    Locals ++ Outsides.

propagate_outside(Event) ->
    %% Push to targets
    {ok, Targets} = application:get_env(spread, targets),
    [spread_gun:send_to(Target, Event) || Target <- Targets].

propagate_locally(Event) ->
    [spread_topic_cache:maybe_add(Event)].
