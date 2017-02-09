-module(spread_core).
-export([
    set_event/5,
    add_data_to_event/3
    ]).

%% Test: spread_core:set_event(<<"fred">>, <<"fred">>, erlang:system_time(microsecond), <<"sefdgd rsgdtgs">>, true, []).

-spec set_event([binary()], binary(), integer(), binary(), boolean()) -> {new | existing, spread_event:event(), [any()]}.
set_event(TopicName, From, Date, DataAsBinary, IsDataFinal) ->
    %% Create the event as stand alone object, store it so it will be handled again if a problem occurs
    {NewOrExisting, Event} = spread_event:new(TopicName, From, Date, DataAsBinary, IsDataFinal),
    %% Propagate that event to local and other servers
    case NewOrExisting of
        existing ->
            {NewOrExisting, Event, []};
        new -> 
            Out = propagate(Event),
            %% Add propagate info to Event object since we're done
            spread_event:set_propagate_info(Event, Out),
            {NewOrExisting, Event, Out}
    end.

add_data_to_event(Event, Data, IsDataFinal) ->
    CurrentData = spread_event:data(Event),
    spread_data:update_with_more_data(CurrentData, Data, IsDataFinal).

-spec propagate(spread_event:event()) -> [too_late | {autotree_app:iteration(), [{[any()], integer()}]}].
propagate(Event) ->
    [spread_topic_cache:maybe_add(Event)].
