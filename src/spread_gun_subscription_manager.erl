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
    Children = supervisor:which_children(?MODULE),
    add_subscription_to_each_child(Children, Sub),
    {reply, ok, State#state{subs = [Sub | State#state.subs]}};
handle_call({get_subs}, _From, State) ->
    {reply, State#state.subs, State};
handle_call(_Request, _From, State) ->
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

add_subscription_to_each_child([Child | Children], Sub) ->
    case Child of
        {_Id, restarting, _Type, _Modules} ->
            add_subscription_to_each_child(Children, Sub);
        {_Id, ChildPid, _Type, _Modules} ->
            gen_server:call(ChildPid, {subscribe, Sub})
    end.
