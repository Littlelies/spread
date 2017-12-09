-module(spread_event).

-export([
    init/0,
    get_all_event_filenames/0,

    get_event_from_filename/1,
    new/7,
    set_propagate_info/2,
    delete_event_file/1,

    topic/1,
    id/1,
    date/1,
    data/1,
    from/1,

    get_maybe_prefix/0
    ]).

-include("spread_storage.hrl").

-type event_id() :: binary().
-type event_date() :: integer().
-type event_from() :: binary().

-record(event, {
    id :: event_id(),
    topic :: spread_topic:topic(),
    from :: event_from(),
    date :: event_date(),
    data :: spread_data:data(),
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
    filelib:ensure_dir(?ROOT_STORAGE_EVENT_DIR ++ "1").

get_all_event_filenames() ->
    filelib:wildcard(?ROOT_STORAGE_EVENT_DIR ++ "*").

-spec get_event_from_filename(list()) -> event() | {error, any()}.
get_event_from_filename(FileName) ->
    FilePath = event_file_path(FileName),
    lager:debug("reading ~p", [FilePath]),
    case file:consult(FilePath) of
        {ok, [Event]} when is_record(Event, event) ->
            Event;
        Any ->
            {error, Any}
    end.

-spec new([binary()], binary(), integer(), binary(), boolean(), boolean(), boolean()) -> { new | existing, event()}.
new(TopicName, From, Date, DataAsBinary, IsDataFinal, IsReal, FailIfExists) ->
    {TopicId, Topic} = spread_topic:new(TopicName),
    Maybe = case FailIfExists of
        true ->
            <<(?MAYBE_PREFIX)/binary, "_">>;
        false ->
            <<>>
    end,
    Id = <<Maybe/binary, TopicId/binary, "_", (integer_to_binary(Date))/binary, "_", From/binary>>,
    case IsReal of
        true ->
            case get_event(Id) of
                {error, _, _} ->
                    Event = make_new_event(Id, From, Date, Topic, DataAsBinary, IsDataFinal),
                    store_event(Event, true),
                    {new, Event};
                Event ->
                    {existing, Event}
            end;
        _ ->
            Event = make_new_event(Id, From, Date, Topic, DataAsBinary, IsDataFinal),
            {new, Event}
    end.

-spec set_propagate_info(event(), any()) -> any().    
set_propagate_info(Event, PropageInfo) ->
    case get_event(Event#event.id) of
        {error, Any, Any2} ->
            lager:error("Failed to get ~p: ~p ~p", [Event#event.id, Any, Any2]),
            error;
        UpdatedEvent ->
            store_event(UpdatedEvent#event{propagate_status = PropageInfo}, false)
    end.

-spec delete_event_file(event()) -> ok | {error, any()}.
delete_event_file(Event) ->
    FileName = event_file_path(Event#event.id, true),
    file:delete(FileName).

get_maybe_prefix() ->
    ?MAYBE_PREFIX.

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
-spec get_event(event_id()) -> event() | {error, any(), any()}.
get_event(EventId) ->
    case get_event_from_filename(binary_to_list(EventId)) of
        {error, Any} ->
            case get_event_from_filename(binary_to_list(EventId) ++ ?TEMP_EXTENSION) of
                {error, Any2} ->
                    {error, Any, Any2};
                Event ->
                    Event
            end;
        Event ->
            Event
    end.

-spec store_event(event(), boolean()) -> ok | {error, any()}.
store_event(Event, IsTemp) when is_record(Event, event) ->
    FileName = event_file_path(Event#event.id, IsTemp),
    spread_utils:write_terms(FileName, [Event]),
    case IsTemp of
        false ->
            file:delete(event_file_path(Event#event.id, true));
        true ->
            ok
    end.

-spec event_file_path(list()) -> list().
event_file_path(FileName) ->
    ?ROOT_STORAGE_EVENT_DIR ++ FileName.

-spec event_file_path(event_id(), boolean()) -> list().
event_file_path(EventId, true) ->
    event_file_path(binary_to_list(EventId) ++ ?TEMP_EXTENSION);
event_file_path(EventId, false) ->
    event_file_path(binary_to_list(EventId)).

make_new_event(Id, From, Date, Topic, DataAsBinary, IsDataFinal) ->
    Data = spread_data:new(Id, DataAsBinary, IsDataFinal),
    #event{
        id = Id,
        from = From,
        date = Date,
        topic = Topic,
        data = Data
    }.
