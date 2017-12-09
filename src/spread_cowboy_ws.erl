%%%-------------------------------------------------------------------
%% @doc spread cowboy SSE interface. Used to stream events at any sub level
%% @end
%%%-------------------------------------------------------------------
-module(spread_cowboy_ws).
-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

-record(state, {
    path :: list(),
    timestamp :: integer()
    }).

%%====================================================================
%% API functions
%%====================================================================

init(Req, _State) ->
    Path = cowboy_req:path_info(Req),
    QsVals = cowboy_req:parse_qs(Req),
    {_, Timestamp} = lists:keyfind(<<"timestamp">>, 1, QsVals),
    {cowboy_websocket, Req, #state{path = Path, timestamp = binary_to_integer(Timestamp)}, 60000, hibernate}.

websocket_init(State) ->
    Timestamp = State#state.timestamp,
    Path = State#state.path,
    lager:debug("~p WS ~p with timestamp ~p", [self(), Path, Timestamp]),
    FirstSet = spread_autotree:subscribe(Path, Timestamp, self()),    
    {reply, {text, spread_autotree:format_updates(FirstSet)}, State}.

websocket_handle(InFrame, State) ->
    lager:debug("Received frame ~p", [InFrame]),
    {reply, pong, State}.

websocket_info({update, PathAsList, Timestamp, Opaque} = Message, State) ->
    lager:debug("~p Received a message ~p", [self(), Message]),
    {reply, {text, spread_autotree:format_updates([{PathAsList, Timestamp, Opaque}])}, State}.

terminate(_Reason, _Req, _State) ->
    lager:debug("Remote closed WS ~p", [_Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
