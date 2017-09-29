-module(spread_cowboy_socket_io).
-export([
    init/2,
    info/3,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

-record(state, {
    path :: list(),
    timestamp :: integer(),
    ttl :: integer()
    }).

-define(TIMEOUT, 600).

%%====================================================================
%% API functions
%%====================================================================

init(Req, _State) ->
    Path = cowboy_req:path_info(Req),

    Timestamp = case cowboy_req:header(<<"last-event-id">>, Req) of
        undefined ->
            0;
        LastEventId ->
            binary_to_integer(LastEventId)
    end,

    QsVals = cowboy_req:parse_qs(Req),
    {_, Transport} = lists:keyfind(<<"transport">>, 1, QsVals),
    case Transport of
        <<"websocket">> ->
            {cowboy_websocket, Req, #state{path = Path, timestamp = Timestamp, ttl = 0}, 60000, hibernate};
        <<"polling">> ->
            Req1 = cowboy_req:stream_reply(200, #{
                <<"content-type">> => <<"text/event-stream">>,
                <<"access-control-allow-origin">> => <<"*">>,
                <<"cache-control">> => <<"no-cache">>,
                <<"x-accel-buffering">> => <<"no">>
            }, Req),
    
            cowboy_req:stream_body(<<"">>, nofin, Req1),

            {cowboy_loop, Req1, #state{ttl = erlang:system_time(second) + ?TIMEOUT, path= Path, timestamp = Timestamp}, hibernate}
    end.

websocket_init(State) ->
    Timestamp = State#state.timestamp,
    Path = State#state.path,
    lager:info("~p WS ~p with timestamp ~p", [self(), Path, Timestamp]),
    FirstSet = spread_autotree:subscribe(Path, Timestamp, self()),    
    {reply, {text, spread_autotree:format_updates(FirstSet)}, State}.

websocket_handle(InFrame, State) ->
    lager:info("Received frame ~p", [InFrame]),
    {reply, pong, State}.

websocket_info({update, PathAsList, Timestamp, Opaque} = Message, State) ->
    lager:info("~p Received a message ~p", [self(), Message]),
    {reply, {text, spread_autotree:format_updates([{PathAsList, Timestamp, Opaque}])}, State}.


info({update, PathAsList, Iteration, Event} = Message, Req, #state{ttl = TTL} = State) ->
    lager:info("~p Received a message ~p", [self(), Message]),
    Now = erlang:system_time(second),
    if
        TTL < Now ->
            lager:info("Dropping connection"),
            {stop, Req, State};
        true ->
            cowboy_req:stream_body(spread_autotree:format_updates([{PathAsList, Iteration, Event}]), nofin, Req),
            {ok, Req, State, hibernate}
    end.

terminate(_Reason, _Req, _State) ->
    lager:info("Remote closed WS ~p", [_Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
