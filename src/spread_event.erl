-module(spread_event).

-export([
    init/0,
    get_all_event_filenames/0,

    get_event/1,
    new/6,
    new/7,
    set_propagate_info/2,

    topic/1,
    id/1,
    date/1,
    data/1,
    from/1
    ]).

-define(ROOT_EVENT_DIR, "./storage/events/").

-type event_id() :: binary().
-type event_date() :: integer().
-type event_from() :: binary().

-record(event, {
    id :: event_id(),
    topic :: spread_topic:topic(),
    from :: event_from(),
    date :: event_date(),
    data :: spread_data:data(),
    propagate_rules = [],
    propagate_status = []
}).
-type event() :: #event{}.


-export_type([event/0, event_id/0, event_date/0]).

%% @todo: keep an index of not yet ended events to retry them when we start
%% @todo: store all events in append only files
%% @todo: add archiving/cleanup mecanisms to reclaim hdd space -> do this in conjonction with cache?
%% @todo: ability to archive this in S3 for example?

%%====================================================================
%% API functions
%%====================================================================

init() ->
    filelib:ensure_dir(?ROOT_EVENT_DIR ++ "1").

get_all_event_filenames() ->
    filelib:wildcard(?ROOT_EVENT_DIR ++ "*").

-spec get_event(event_id()) -> event() | {error, any()}.
get_event(EventId) ->
    FileName = event_filename(EventId),
    case file:consult(FileName) of
        {ok, [Event]} when is_record(Event, event) ->
            Event;
        Any ->
            {error, Any}
    end.

-spec new([binary()], binary(), integer(), binary(), boolean(), [any()]) -> { new | existing, event()}.
new(TopicName, From, Date, DataAsBinary, IsDataFinal, SpecificPropagateRules) ->
    new(TopicName, From, Date, DataAsBinary, IsDataFinal, SpecificPropagateRules, true).

-spec new([binary()], binary(), integer(), binary(), boolean(), [any()], boolean()) -> { new | existing, event()}.
new(TopicName, From, Date, DataAsBinary, IsDataFinal, SpecificPropagateRules, IsReal) ->
    {TopicId, Topic} = spread_topic:new(TopicName),
    Id = <<TopicId/binary, "_", (integer_to_binary(Date))/binary, "_", From/binary>>,
    case IsReal of
        true ->
            case get_event(Id) of
                {error, _} ->
                    Event = make_new_event(Id, From, Date, Topic, DataAsBinary, IsDataFinal, SpecificPropagateRules),
                    store_event(Event),
                    {new, Event};
                Event ->
                    {existing, Event}
            end;
        _ ->
            Event = make_new_event(Id, From, Date, Topic, DataAsBinary, IsDataFinal, SpecificPropagateRules),
            {new, Event}
    end.

-spec set_propagate_info(event(), any()) -> any().    
set_propagate_info(Event, PropageInfo) ->
    UpdatedEvent = get_event(Event#event.id),
    store_event(UpdatedEvent#event{propagate_status = PropageInfo}).

%%====================================================================
%% Accessors
%%====================================================================

topic(Event) ->
    Event#event.topic.

id(Event) ->
    Event#event.id.

date(Event) ->
    Event#event.date.

data(Event) ->
    Event#event.data.

from(Event) ->
    Event#event.from.

%%====================================================================
%% Internal functions
%%====================================================================

-spec store_event(event()) -> any().
store_event(Event) when is_record(Event, event) ->
    FileName = event_filename(Event#event.id),
    spread_utils:write_terms(FileName, [Event]).

-spec event_filename(event_id()) -> list().
event_filename(EventId) ->
    ?ROOT_EVENT_DIR ++ binary_to_list(EventId).

make_new_event(Id, From, Date, Topic, DataAsBinary, IsDataFinal, SpecificPropagateRules) ->
    Data = spread_data:new(Id, DataAsBinary, IsDataFinal),
    #event{
        id = Id,
        from = From,
        date = Date,
        topic = Topic,
        data = Data,
        propagate_rules = SpecificPropagateRules
    }.
