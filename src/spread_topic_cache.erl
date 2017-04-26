%%%-------------------------------------------------------------------
%% @doc spread topic cache. Supposed to be fast for lookups
%% So we use autotree
%% When we start, we look into cached events, only keep latest ones, fill autotree
%% @todo: Attempt to propagate again tmp files? No need, it will be handled automatically, since we restart. But file won't be renamed. SO we just need to remove tmp files manually.
%% @end
%%%-------------------------------------------------------------------
-module(spread_topic_cache).

-include_lib("kernel/include/file.hrl").

-export([maybe_add/2, get_latest/1]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-include("spread_storage.hrl").

%%====================================================================
%% API functions
%%====================================================================

-spec maybe_add(spread_event:event() | {error, any()}, boolean()) -> {autotree_app:iteration(), [{[any()], integer()}], spread_event:event() | error} | {too_late, spread_event:event()} | {error, any()}.
maybe_add({error, Any}, _FailIfExists) ->
    {error, Any};
maybe_add(Event, FailIfExists) ->
    spread_autotree:update(Event, FailIfExists).

-spec get_latest(spread_topic:topic_name()) -> {integer(), spread_event:event()} | error.
get_latest(TopicName) ->
    spread_autotree:get_iteration_and_opaque(TopicName).

%%====================================================================
%% gen_server callbacks
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    filelib:ensure_dir(?ROOT_HISTORY_DATA_DIR ++ "/1"),
    filelib:ensure_dir(?ROOT_HISTORY_EVENT_DIR ++ "/1"),
    lager:info("Getting all local events"),
    AllEventFilenames = spread_event:get_all_event_filenames(),
    lager:info("Filtering all local events ~p", [AllEventFilenames]),
    {LatestFiles, OlderFiles} = filter_out_old_events_per_topic(AllEventFilenames, []),

    %% We are starting, meaning no one is reading the old files
    do_move_history(OlderFiles),

    lager:info("Filling cache ~p", [LatestFiles]),
    [maybe_add(spread_event:get_event_from_filename(LatestFile), false) || LatestFile <- LatestFiles],
    lager:info("Filling DONE"),
    %% Fill the cache from the list of events
    erlang:send_after(60 * 1000, self(), {gc}),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gc}, State) ->
    move_history(),
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

filter_out_old_events_per_topic([], Acc) ->
    {[erase_temp_file(TopicId) || {temp, TopicId} <- get_keys()], Acc};
filter_out_old_events_per_topic([File | Files], Acc) ->
    NewAcc = case re:split(File, "_", [{return, binary}]) of
        [?MAYBE_PREFIX, TopicId, Timestamp | _From] ->
            case get({temp, TopicId}) of
                undefined ->
                    put({temp, TopicId}, {Timestamp, File, maybe}),
                    Acc;
                {_OtherTimestamp, _Other} -> % Fail because there is another event on it (we don't check whether it is after of before, result is the same)
                    [File | Acc];
                {OtherTimestamp, Other, maybe} -> % Should not happen
                    lager:error("Edge case: 2 maybe events, please report ~p", [TopicId]),
                    if
                        OtherTimestamp < Timestamp ->
                            put({temp, TopicId}, {Timestamp, File, maybe}),
                            [Other | Acc];
                        true ->
                            [File | Acc]
                    end
            end;
        [TopicId, Timestamp | _From] ->
            case get({temp, TopicId}) of
                {OtherTimestamp, Other} ->
                    if
                        OtherTimestamp < Timestamp ->
                            put({temp, TopicId}, {Timestamp, File}),
                            [Other | Acc];
                        true ->
                            [File | Acc]
                    end;
                {_OtherTimestamp, Other, maybe} ->
                    put({temp, TopicId}, {Timestamp, File}),
                    [Other | Acc];
                undefined ->
                    put({temp, TopicId}, {Timestamp, File}),
                    Acc                 
            end
    end,
    filter_out_old_events_per_topic(Files, NewAcc).

erase_temp_file(TopicId) ->
    {_Timestamp, File} = erase({temp, TopicId}),
    lists:last(re:split(File, "/", [{return, list}])).

move_history() ->
    %% TODO : make sure old files are NEVER opened
    %% Get all old files
    AllEventFilenames = spread_event:get_all_event_filenames(),
    {_, OldFiles} = filter_out_old_events_per_topic(AllEventFilenames, []),
    %% Get all opened big files
    FreeFiles = filter_out_open_data(OldFiles),
    %% Move them
    do_move_history(FreeFiles).

do_move_history(FreeFiles) ->
    lager:info("MOVING ~p", [FreeFiles]),
    [maybe_move_file_and_its_data(maybe_add_local(File)) || File <- FreeFiles].

filter_out_open_data(OldFiles) ->
    Open = insert_all_open_files(),
    lager:info("OPEN ~p", [Open]),
    Out = do_filter_out_open_data(OldFiles, []),
    flush_all_open_files(),
    Out.

insert_all_open_files() ->
    List = re:split(os:cmd("lsof +d " ++ ?ROOT_STORAGE_DATA_DIR ++ " | tail -n +2 | awk '{print $9}'"), "\n", [{return, list}]),
    [put_file_in_dict(L) || L  <- List].

flush_all_open_files() ->
    [erase({temp_open, L}) || {temp_open, L} <- get_keys()].

do_filter_out_open_data([], Acc) ->
    Acc;
do_filter_out_open_data([OldFile | OldFiles], Acc) ->
    case get({temp_open, OldFile}) of
        undefined ->
            do_filter_out_open_data(OldFiles, [OldFile | Acc]);
        _ ->
            do_filter_out_open_data(OldFiles, Acc)
    end.

data_to_event(L) ->
    re:replace(L, ?DATA_SUB_PATH, ?EVENT_SUB_PATH, [{return, list}]).

maybe_add_local([$/ | _] = FilePath) ->
    FilePath;
maybe_add_local(FilePath) ->
    "./" ++ FilePath.

maybe_move_file_and_its_data(File) ->
    case re:split(File, ?TEMP_EXTENSION, [{return, list}]) of
        [_] ->    
            file:rename(File, re:replace(File, ?STORAGE_PATH, ?HISTORY_PATH, [{return, list}])),
            DataFile = re:replace(File, ?EVENT_SUB_PATH, ?DATA_SUB_PATH, [{return, list}]),
            file:rename(DataFile, re:replace(DataFile, ?STORAGE_PATH, ?HISTORY_PATH, [{return, list}])),
            {moved, File};
        _ ->
            lager:info("tmp event, do not move it"),
            {skipped, File}
    end.

put_file_in_dict(L) ->
    EventFileName = data_to_event(L),
    put({temp_open, EventFileName}, EventFileName),
    EventFileName.
