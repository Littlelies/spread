-module(spread_autotree).

-export([
    subscribe/3,
    update/1,
    get_timestamp_and_opaque/1,
    format_updates/1,
    parse_updates_and_broadcast/2
]).

subscribe(Path, Timestamp, Pid) ->
    autotree_app:subscribe(Path, Timestamp, Pid).

update(Event) ->
    PathAsList = spread_topic:name(spread_event:topic(Event)),
    Timestamp = spread_event:date(Event),
    lager:info("Update ~p", [PathAsList]),
    autotree_app:update(PathAsList, Timestamp, Event).

get_timestamp_and_opaque(TopicName) ->
    autotree_app:get_timestamp_and_opaque(TopicName).

-spec format_updates(list()) -> binary().
format_updates([]) -> <<>>;
format_updates([A | Others]) ->
    <<(format_update(A))/binary, (format_updates(Others))/binary>>.

-spec parse_updates_and_broadcast(binary(), fun()) -> {binary(), [spread_event:event()]}.
parse_updates_and_broadcast(Binary, Callback) ->
    Updates = binary:split(Binary, <<"\n\n">>, [global]),
    parse_updates_and_broadcast(Updates, Callback, []).

%%====================================================================
%% Internal functions
%%====================================================================


format_update({PathAsList, Timestamp, Event}) ->
    <<"id: ", (integer_to_binary(Timestamp))/binary,
        "\ndata: ", (spread_utils:binary_join(PathAsList, <<"/">>))/binary ,
        "\ndata: ", (spread_event:from(Event))/binary ,
        "\ndata: ", (spread_data:raw(spread_event:data(Event)))/binary, "\n\n">>.

parse_updates_and_broadcast([PartialUpdate], _Callback, Acc) ->
    {PartialUpdate, lists:reverse(Acc)};
parse_updates_and_broadcast([Update | Updates], Callback, Acc) ->
    [<<"id: ", TimestampB/binary>>, A] = binary:split(Update, <<"\n">>),
    [<<"data: ", PathB/binary>>, B] = binary:split(A, <<"\n">>),
    [<<"data: ", From/binary>>, <<"data: ", Data/binary>>] = binary:split(B, <<"\n">>),
    Date = binary_to_integer(TimestampB),
    IsFile = case Data of
        <<>> ->
            lager:info("this is a file"),
            true;
        _ ->
            false
    end,
    {IsNew, Event, _} = spread_core:set_event(spread_topic:binary_to_name(PathB), From, Date, Data, IsFile, []),
    case {IsNew, IsFile} of
        {new, true} ->
            Callback(Event);
        _ ->
            lager:info("No new file, no need to download ~p", [{IsNew, IsFile}]),
            ok
    end,
    parse_updates_and_broadcast(Updates, Callback, [Event | Acc]).
