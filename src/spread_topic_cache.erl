%%%-------------------------------------------------------------------
%% @doc spread topic cache. Supposed to be fast for lookups
%% So we use ets for concurrent read access
%% Writes need a lookup first, so they are processed sequentially via gen_server
%% They are also logged to disk
%% @end
%%%-------------------------------------------------------------------
-module(spread_topic_cache).

-include_lib("kernel/include/file.hrl").

-export([maybe_add/1, get_latest/1]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%====================================================================
%% API functions
%%====================================================================

-spec maybe_add(spread_event:event()) -> list() | too_late | {error, any()}.
maybe_add(Event) ->
    spread_autotree:update(Event).

-spec get_latest(spread_topic:topic_name()) -> {integer(), spread_event:event()} | error.
get_latest(TopicName) ->
    spread_autotree:get_timestamp_and_opaque(TopicName).

%%====================================================================
%% gen_server callbacks
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    lager:info("Getting all local events"),
    AllEventFilenames = spread_event:get_all_event_filenames(),
    lager:info("Filtering all local events ~p", [AllEventFilenames]),
    LastEventIds = filter_out_old_events_per_topic(AllEventFilenames),
    lager:info("Filling cache ~p", [LastEventIds]),
    [maybe_add(spread_event:get_event(EventId)) || EventId <- LastEventIds],
    lager:info("Filling DONE"),
    %% Fill the cache from the list of events
    erlang:send_after(60 * 1000, self(), {gc}),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gc}, State) ->
    erlang:garbage_collect(self()),
    erlang:send_after(60 * 1000, self(), {gc}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
filter_out_old_events_per_topic([]) ->
    lager:info("Keys are ~p", [get_keys()]),
    [erase_temp_file(TopicId) || {temp, TopicId} <- get_keys()];
filter_out_old_events_per_topic([File | Files]) ->
    [TopicId, Timestamp | _From] = re:split(File, "_", [{return, binary}]),
    case get({temp,TopicId}) of
        undefined ->
            put({temp, TopicId}, {Timestamp, File});
        {OtherTimestamp, _Other} ->
            if
                OtherTimestamp < Timestamp ->
                    put({temp, TopicId}, {Timestamp, File});
                true ->
                    ok
            end
    end,
    filter_out_old_events_per_topic(Files).

erase_temp_file(TopicId) ->
    {_Timestamp, File} = erase({temp, TopicId}),
    lists:last(re:split(File, "/", [{return, binary}])).
