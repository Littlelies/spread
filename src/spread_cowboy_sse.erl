%%%-------------------------------------------------------------------
%% @doc spread cowboy SSE interface. Used to stream events at any sub level
%% @end
%%%-------------------------------------------------------------------
-module(spread_cowboy_sse).
-export([
    init/2,
    info/3,
    terminate/3,
    parse_updates/1
]).

%%====================================================================
%% API functions
%%====================================================================

init(Req, State) ->
    Path = cowboy_req:path_info(Req),

    Timestamp = case cowboy_req:header(<<"last-event-id">>, Req) of
        undefined ->
            0;
        LastEventId ->
            binary_to_integer(LastEventId)
    end,

    lager:info("~p SSE ~p with timestamp ~p", [self(), Path, Timestamp]),

    FirstSet = autotree_app:subscribe(Path, Timestamp, self()),
    
    Req1 = cowboy_req:stream_reply(200, #{
        <<"Content-Type">> => <<"text/event-stream">>        
    }, Req),
    
    cowboy_req:stream_body(format_updates(FirstSet), nofin, Req1),
    
    {cowboy_loop, Req1, State, 60000, hibernate}.

info({update, PathAsList, Timestamp, Event} = Message, Req, State) ->
    lager:info("~p Received a message ~p", [self(), Message]),
    cowboy_req:stream_body(format_update({PathAsList, Timestamp, Event}), nofin, Req),
    {ok, Req, State}.

terminate(_Reason, _Req, _State) ->
    ok.

parse_updates(Binary) ->
    Updates = binary:split(Binary, <<"\n\n">>, [global]),
    parse_updates(Updates, []).

%%====================================================================
%% Internal functions
%%====================================================================

format_updates([]) -> <<>>;
format_updates([A | Others]) ->
    <<(format_update(A))/binary, (format_updates(Others))/binary>>.

format_update({PathAsList, Timestamp, Event}) ->
    <<"id: ", (integer_to_binary(Timestamp))/binary, "\ndata: ", (spread_utils:binary_join(PathAsList, <<"/">>))/binary , "\ndata: ", (spread_data:raw(spread_event:data(Event)))/binary, "\n\n">>.

parse_updates([PartialUpdate], Acc) ->
    {PartialUpdate, lists:reverse(Acc)};
parse_updates([Update | Updates]) ->
    [<<"id: ", TimestampB/binary>>, A] = binary:split(Update, <<"\n">>),
    [<<"data: ", PathB/binary>>, <<"data: ", Data/binary>>] = binary:split(A, <<"\n">>),
    parse_updates(Updates, [{PathB, binary_to_integer(TimestampB), Data} | Acc]).
