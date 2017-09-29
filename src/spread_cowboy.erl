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
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/sse/[...]", spread_cowboy_sse, []},
            {"/ws/[...]", spread_cowboy_ws, []},
            {"/socket.io/[...]", spread_cowboy_socket_io, []},
            {"/jwttoken", spread_jwt_auth, []},
            {"/raw/[...]", spread_cowboy_http, []},
            {"/", cowboy_static, {file, "/var/www/html/index.html"}},
            {"/[...]", cowboy_static, {dir, "/var/www/html"}}
        ]}
    ]),
    cowboy:start_clear(http_listener,
        [{port, 8080}],
        #{
            env => #{dispatch => Dispatch}
            ,request_timeout => 60000
            %,stream_handlers => [cowboy_compress_h, cowboy_stream_h]
        }
    ).

get_auth(Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        <<"Bearer ", Authorization/binary>> ->
            spread_jwt_auth:auth(Authorization);
        _ ->
            lager:info("Unsecure connection, should be refusing it"),
            error
    end.
