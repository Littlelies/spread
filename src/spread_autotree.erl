-module(spread_autotree).

-export([
    subscribe/3,
    update/2,
    get_iteration_and_opaque/1,
    format_updates/1,
    parse_updates_and_broadcast/2
]).

-spec subscribe([binary()], autotree_app:iteration(), pid()) -> [{[any()], autotree_app:iteration(), any()}].
subscribe(Path, Timestamp, Pid) ->
    autotree_app:subscribe(Path, Timestamp, Pid).

-spec update(spread_event:event(), boolean()) -> {too_late, spread_event:event()} | {autotree_app:iteration(), [{[any()], integer()}], spread_event:event() | error}.
update(Event, FailIfExists) ->
    PathAsList = spread_topic:name(spread_event:topic(Event)),
    lager:info("Update ~p", [PathAsList]),
    autotree_app:update(PathAsList, Event, FailIfExists).

-spec get_iteration_and_opaque([binary()]) -> {autotree_app:iteration(), any()} | error.
get_iteration_and_opaque(TopicName) ->
    autotree_app:get_iteration_and_opaque(TopicName).

-spec format_updates(list()) -> binary().
format_updates([]) -> <<>>;
format_updates(Updates) -> format_updates(Updates, undefined).

format_updates([A], undefined) ->
    format_update(A);
format_updates([{PathAsList, _Iteration, Event}], LastIteration) ->
    format_update({PathAsList, LastIteration, Event});
format_updates([{PathAsList, Iteration, Event} | Others], LastIteration) ->
    if
        LastIteration < Iteration ->
            <<(format_update({PathAsList, Iteration, Event}))/binary, (format_updates(Others, Iteration))/binary>>;
        true ->
            <<(format_update({PathAsList, Iteration, Event}))/binary, (format_updates(Others, LastIteration))/binary>>
    end.

-spec parse_updates_and_broadcast(binary(), fun()) -> {binary(), autotree:iteration(), [spread_event:event()]}.
parse_updates_and_broadcast(Binary, Callback) ->
    Updates = binary:split(Binary, <<"\n\n">>, [global]),
    parse_updates_and_broadcast(Updates, Callback, [], <<"0">>).

%%====================================================================
%% Internal functions
%%====================================================================

format_update({PathAsList, Iteration, Event}) ->
    <<"id: ", (integer_to_binary(Iteration))/binary,
        "\ndata: ", (spread_utils:binary_join(PathAsList))/binary,
        "\ndata: ", (spread_event:from(Event))/binary,
        "\ndata: ", (integer_to_binary(spread_event:date(Event)))/binary,
        "\ndata: ", (spread_data:raw(spread_event:data(Event)))/binary, "\n\n">>.

parse_updates_and_broadcast([PartialUpdate], _Callback, Acc, Iteration) ->
    {PartialUpdate, binary_to_integer(Iteration), lists:reverse(Acc)};
parse_updates_and_broadcast([Update | Updates], Callback, Acc, _) ->
    [<<"id: ", Iteration/binary>>, A] = binary:split(Update, <<"\n">>),
    [<<"data: ", PathB/binary>>, B] = binary:split(A, <<"\n">>),
    [<<"data: ", From/binary>>, C] = binary:split(B, <<"\n">>),
    [<<"data: ", TimestampB/binary>>, <<"data: ", Data/binary>>] = binary:split(C, <<"\n">>),
    Date = binary_to_integer(TimestampB),
    IsFile = case Data of
        <<>> ->
            lager:info("this is a file"),
            true;
        _ ->
            false
    end,
    {IsNew, Event, _, _} = spread_core:set_event(spread_topic:binary_to_name(PathB), From, Date, Data, not IsFile, false),
    case {IsNew, IsFile} of
        {new, true} ->
            Callback(Event);
        _ ->
            lager:info("No new file, no need to download ~p", [{IsNew, IsFile}]),
            ok
    end,
    parse_updates_and_broadcast(Updates, Callback, [Event | Acc], Iteration).

