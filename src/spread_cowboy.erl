%%%-------------------------------------------------------------------
%% @doc spread cowboy interface. 
%% @end
%%%-------------------------------------------------------------------
-module(spread_cowboy).

-export([
    start/0,
    get_auth/1
]).

%%====================================================================
%% API functions
%%====================================================================

start() ->
    lager:info("Starting cowboy"),
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/sse/[...]", spread_cowboy_sse, []},
            {"/ws/[...]", spread_cowboy_ws, []},
            {"/", cowboy_static, {file, "/Users/fredericminot/spread/index.html"}},
            {"/raw/[...]", spread_cowboy_http, []}
        ]}
    ]),
    cowboy:start_clear(http_listener, 100,
        [{port, 8080}],
        #{env => #{dispatch => Dispatch}, request_timeout => 60000}
    ).

get_auth(Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        <<"Bearer ", Authorization/binary>> ->
            spread_jwt_auth:auth(Authorization);
        _ ->
            lager:info("Unsecure connection, should be refusing it"),
            error
    end.
