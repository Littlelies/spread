-module(spread_gun).

-behaviour(supervisor).

%% API
-export([start_link/0
        ]).
-export([add_connection/1, send_to/2]).

%% Supervisor callbacks
-export([init/1]).



%% Helper macro for declaring children of supervisor
%%-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_connection(Target) ->
    Child = {Target, {spread_gun_connection, start_link, [Target]}, permanent, 5000, worker, [spread_gun_connection]},
    case supervisor:start_child(?MODULE, Child) of
        {ok, ChildPid} ->
            lager:info("Started gun connection to ~p at ~p", [Target, ChildPid]),
            ChildPid;
        Error ->
            lager:error("Failed to start gun connection to ~p with reason ~p", [Target, Error]),
            error
    end.

send_to(Target, Event) ->
    Children = supervisor:which_children(?MODULE),
    Out = case lists:keyfind(Target, 1, Children) of
        false ->
            lager:error("Attempt to find missing child ~p", [Target]),
            case add_connection(Target) of
                error ->
                    error;
                ChildPid ->
                    gen_server:call(ChildPid, {send, Event, self()})
            end;
        {_Id, restarting, _Type, _Modules} ->
            lager:error("Attempt to send to a restarting child ~p", [Target]),
            error;
        {_Id, ChildPid, _Type, _Modules} ->
            gen_server:call(ChildPid, {send, Event, self()})
    end,
    case Out of
        ok ->
            Target;
        _ ->
            lager:error("Error sending to ~p: ~p", [Target, Out]),
            error
    end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    PermanentTargets = [],
    Children = [{Target, {spread_gun_connection, start_link, [Target]}, permanent, 5000, worker, [spread_gun_connection]} || Target <- PermanentTargets],
    {ok, { {one_for_one, 5, 1}, Children} }.
