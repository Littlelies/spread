-module(spread_core).
-export([
    set_event/6,
    add_data_to_event/3
    ]).

%% Test: spread_core:set_event(<<"fred">>, <<"fred">>, erlang:system_time(microsecond), <<"sefdgd rsgdtgs">>, true, []).

-spec set_event([binary()], binary(), integer(), binary(), boolean(), any()) -> {spread_event:event(), [any()]}.
set_event(TopicName, From, Date, DataAsBinary, IsDataFinal, SpecificPropagateRules) ->
    %% Create the event as stand alone object, store it so it will be handled again if a problem occurs
    {_NewOrExisting, Event} = spread_event:new(TopicName, From, Date, DataAsBinary, IsDataFinal, SpecificPropagateRules),
    %% Propagate that event to local and other servers
    Out = propagate(Event),
    %% Add propagate info to Event object since we're done
    spread_event:set_propagate_info(Event, Out),
    {Event, Out}.

add_data_to_event(Event, Data, IsDataFinal) ->
    CurrentData = spread_event:data(Event),
    spread_data:update_with_more_data(CurrentData, Data, IsDataFinal).

propagate(Event) ->
    [spread_topic_cache:maybe_add(Event)].
