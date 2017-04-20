-module(spread_core).
-export([
    set_event/6,
    add_data_to_event/3
    ]).

%% Test: spread_core:set_event(<<"fred">>, <<"fred">>, erlang:system_time(microsecond), <<"sefdgd rsgdtgs">>, true, []).

-spec set_event([binary()], binary(), integer(), binary(), boolean(), boolean()) -> {new | existing, spread_event:event(), {too_late, spread_event:event()} | {autotree_app:iteration(), [{[any()], integer()}], spread_event:event() | error}, spread_event:event() | error}.
set_event(TopicName, From, Date, DataAsBinary, IsDataFinal, FailIfExists) ->
    %% Create the event as stand alone object, store it so it will be handled again if a problem occurs
    {NewOrExisting, Event} = spread_event:new(TopicName, From, Date, DataAsBinary, IsDataFinal, true, FailIfExists),
    %% Propagate that event to subscribers
    case NewOrExisting of
        existing ->
            {NewOrExisting, Event, [], Event};
        new -> 
            Out = propagate(Event, FailIfExists),
            PreviousEventOrError = get_previous_event(Out),
            case FailIfExists of
                true when PreviousEventOrError =/= error->
                    spread_event:delete_event_file(Event);
                _ ->
                    %% Add propagate info to Event object since we're done
                    spread_event:set_propagate_info(Event, Out)
            end,
            {NewOrExisting, Event, Out, PreviousEventOrError}
    end.

add_data_to_event(Event, Data, IsDataFinal) ->
    CurrentData = spread_event:data(Event),
    spread_data:update_with_more_data(CurrentData, Data, IsDataFinal).

-spec propagate(spread_event:event(), boolean()) -> {too_late, spread_event:event()} | {autotree_app:iteration(), [{[any()], integer()}], spread_event:event() | error}.
propagate(Event, FailIfExists) ->
    spread_topic_cache:maybe_add(Event, FailIfExists).

get_previous_event({too_late, Event}) ->
    Event;
get_previous_event({_Iteration, _PropagationReport, error}) ->
    error;
get_previous_event({_Iteration, _PropagationReport, Event}) ->
    Event.

