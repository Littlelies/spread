-module(spread_gun).

-behaviour(supervisor).

%% API
-export([start_link/0
        ]).
-export([add_connection/1
%    , send_to/2
]).

-export([subscribe/1]).

%% Supervisor callbacks
-export([init/1]).

%% @todo: add prefix to children ids to avoid duplicates for other code, since they are registered now

%% Helper macro for declaring children of supervisor
%%-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec add_connection(atom()) -> {new | existing, spread_event:event(), [any()]} | {error, any()}.
add_connection(Target) ->
    spread_gun_peers_manager:add_connection(Target).

-spec subscribe(spread_sub:sub()) -> ok.
subscribe(Sub) ->
    %% Send subscribe to all children now, and to any new connection
    spread_gun_subscription_manager:add_subscription(Sub).

% send_to(Target, Event) ->
%     Children = supervisor:which_children(?MODULE),
%     Out = case lists:keyfind(Target, 1, Children) of
%         false ->
%             lager:error("Attempt to find missing child ~p", [Target]),
%             case add_connection(Target) of
%                 error ->
%                     error;
%                 ChildPid ->
%                     gen_server:call(ChildPid, {send, Event, self()})
%             end;
%         {_Id, restarting, _Type, _Modules} ->
%             lager:error("Attempt to send to a restarting child ~p", [Target]),
%             error;
%         {_Id, ChildPid, _Type, _Modules} ->
%             gen_server:call(ChildPid, {send, Event, self()})
%     end,
%     case Out of
%         ok ->
%             Target;
%         _ ->
%             lager:error("Error sending to ~p: ~p", [Target, Out]),
%             error
%     end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    RemotePeersManager = {spread_gun_peers_manager, {spread_gun_peers_manager, start_link, []}, permanent, 5000, worker, [spread_gun_peers_manager]},
    RemoteSubsManager = {spread_gun_subscription_manager, {spread_gun_subscription_manager, start_link, []}, permanent, 5000, worker, [spread_gun_subscription_manager]},
%    PermanentTargets = spread:subscribe_locally([<<"peers">>], Pid),
%    Children = [{Target, {spread_gun_connection, start_link, [Target]}, permanent, 5000, worker, [spread_gun_connection]} || Target <- PermanentTargets],
    {ok, { {one_for_one, 5, 1}, [RemotePeersManager, RemoteSubsManager]} }.
