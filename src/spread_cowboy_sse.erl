%%%-------------------------------------------------------------------
%% @doc spread cowboy SSE interface. Used to stream events at any sub level
%% @end
%%%-------------------------------------------------------------------
-module(spread_cowboy_sse).
-export([
    init/2,
    info/3,
    terminate/3
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

    FirstSet = spread_autotree:subscribe(Path, Timestamp, self()),
    
    %% Note: nginx config must have proxy_http_version 1.1; and proxy_set_header Connection "";

    Req1 = cowboy_req:stream_reply(200, #{
        <<"content-type">> => <<"text/event-stream">>,
        <<"access-control-allow-origin">> => <<"*">>,
        <<"cache-control">> => <<"no-cache">>,
        <<"x-accel-buffering">> => <<"no">>
    }, Req),
    
    cowboy_req:stream_body(spread_autotree:format_updates(FirstSet), nofin, Req1),
    
    {cowboy_loop, Req1, State, hibernate}.

info({update, PathAsList, Iteration, Event} = Message, Req, State) ->
    lager:info("~p Received a message ~p", [self(), Message]),
    cowboy_req:stream_body(spread_autotree:format_updates([{PathAsList, Iteration, Event}]), nofin, Req),
    {ok, Req, State}.

terminate(_Reason, _Req, _State) ->
    ok.
