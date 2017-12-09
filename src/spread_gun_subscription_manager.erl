-module(spread_gun_subscription_manager).

-behaviour(gen_server).

%% API
-export([add_subscription/1]).
-export([get_subs/0]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    subs = []       %% Keep track of the list of current subscriptions, maybe turn this into an ets set?
}).

%% ===================================================================
%% API functions
%% ===================================================================

add_subscription(Sub) ->
    gen_server:call(?MODULE, {add_subscription, Sub}).

get_subs() ->
    gen_server:call(?MODULE, {get_subs}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

handle_call({add_subscription, Sub}, _From, State) ->
    Pids = spread_gun_peers_manager:get_connections_pids(),
    add_subscription_to_each_connection(Pids, Sub),
    {reply, ok, State#state{subs = [Sub | State#state.subs]}};
handle_call({get_subs}, _From, State) ->
    {reply, State#state.subs, State};
handle_call(_Request, _From, State) ->
    lager:debug("Unknown call ~p", [_Request]),
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
add_subscription_to_each_connection([], _Sub) ->
    ok;
add_subscription_to_each_connection([Pid | Pids], Sub) ->
    catch gen_server:cast(Pid, {subscribe, Sub}),
    add_subscription_to_each_connection(Pids, Sub).
