-module(spread_gun_peers_manager).

-behaviour(gen_server).

-export([
    add_connection/2,
    get_connections_pids/0
]).

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

add_connection(Target, Auth) ->
    spread:post([?PEERS_ROOT_PATH, atom_to_binary(Target, utf8)], Auth).

get_connections_pids() ->
    lager:debug("get connection pids ~p", [self()]),
    Out = [Pid || {_Id, Pid, _Type, [spread_gun_connection]} <- supervisor:which_children(spread_gun)],
    lager:debug("out is ~p", [Out]),
    Out.

start_link() ->
    Out = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
    lager:debug("started"),
    Out.

init([]) ->
    PermanentTargets = spread:subscribe_locally([?PEERS_ROOT_PATH], self()),
    [self() ! {add_peer, Target, spread_data:to_binary(spread_event:data(Event), self())} || {[?PEERS_ROOT_PATH, Target], _, Event} <- PermanentTargets],
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    lager:debug("Unknown cast ~p", [_Msg]),
    {noreply, State}.

handle_info({add_peer, Peer, Auth}, State) ->
    add_connection_on_sup(binary_to_atom(Peer, utf8), Auth),
    {noreply, State};
handle_info({update, [?PEERS_ROOT_PATH, Peer], _Timestamp, Event}, State) ->
    Auth = spread_data:to_binary(spread_event:data(Event), self()),
    add_connection_on_sup(binary_to_atom(Peer, utf8), Auth),
    {noreply, State};
handle_info(_Info, State) ->
    lager:debug("Unknown info ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

add_connection_on_sup(Target, Auth) ->
    Child = {Target, {spread_gun_connection, start_link, [Target, Auth]}, permanent, 5000, worker, [spread_gun_connection]},
    lager:debug("Starting child ~p ~p", [Target, self()]),
    case supervisor:start_child(spread_gun, Child) of
        {ok, ChildPid} ->
            lager:info("Started gun connection to ~p at ~p", [Target, ChildPid]),
            ChildPid;
        Error ->
            lager:error("Failed to start gun connection to ~p with reason ~p", [Target, Error]),
            error
    end.
