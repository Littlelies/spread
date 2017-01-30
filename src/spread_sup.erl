%%%-------------------------------------------------------------------
%% @doc spread top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(spread_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    Data = {spread_data, {spread_data, start_link, []}, permanent, 5000, worker, [spread_data]},

    Cache = {spread_topic_cache, {spread_topic_cache, start_link, []}, permanent, 5000, worker, [spread_topic_cache]},

    Gun = {spread_gun, {spread_gun, start_link, []}, permanent, 5000, supervisor, [spread_gun]},

    %% There is no point in having a server if Spread is not running
    Cowboy = {spread_cowboy, {spread_cowboy, start, []}, permanent, 5000, supervisor, [spread_cowboy]},

    {ok, { {one_for_all, 1, 5}, [
        Data, Cache,        %% Core
        Gun, Cowboy]} }.    %% Interfaces

%%====================================================================
%% Internal functions
%%====================================================================
