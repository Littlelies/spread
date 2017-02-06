-module(spread_gun_peers_manager).

-behaviour(gen_server).

-export([add_connection/1]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(PEERS_ROOT_PATH, <<"local">>, <<"peers">>).

add_connection(Target) ->
    spread:post([?PEERS_ROOT_PATH, atom_to_binary(Target, utf8)], <<"pending">>).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    PermanentTargets = spread:subscribe_locally([<<"peers">>], self()),
    [add_connection_on_sup(Target) || Target <- PermanentTargets],
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    lager:info("Unknown cast ~p", [_Msg]),
    {noreply, State}.

handle_info({update, [?PEERS_ROOT_PATH, Peer], _Timestamp, _Event}, State) ->
    lager:info("New update on peer ~p", [Peer]),
    add_connection_on_sup(binary_to_atom(Peer, utf8)),
    {noreply, State};
handle_info(_Info, State) ->
    lager:info("Unknown info ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

add_connection_on_sup(Target) ->
    Child = {Target, {spread_gun_connection, start_link, [Target]}, permanent, 5000, worker, [spread_gun_connection]},
    case supervisor:start_child(?MODULE, Child) of
        {ok, ChildPid} ->
            lager:info("Started gun connection to ~p at ~p", [Target, ChildPid]),
            ChildPid;
        Error ->
            lager:error("Failed to start gun connection to ~p with reason ~p", [Target, Error]),
            error
    end.
