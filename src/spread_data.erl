%%%-------------------------------------------------------------------
%% @doc spread data
%% A file is either writing, ready for new writes, or done.
%% It means each file object we have is only for files we created and being filled
%% Other files are supposed to be closed
%% @end
%%%-------------------------------------------------------------------
-module(spread_data).

%% API
-export([
    new/3, 
    raw/1,
    to_binary/2,
    update_with_more_data/3
    ]).

-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
    ]).

-include_lib("kernel/include/file.hrl").


-type data_type() :: file | raw.
-record(data, {
    type :: data_type(),
    payload :: binary() | list()
}).
-type data() :: #data{}.

-export_type([data/0, data_type/0]).

-record(state, {}). %% Files are stored in process dictionary

-include("spread_storage.hrl").

-record(file, {
    fd                  :: file:fd(),
    size                :: integer(),
    state = ready       :: writing | ready | done,
    subscribers = [],
    writers = []
    }).

%%====================================================================
%% API functions
%%====================================================================

-spec new(spread_event:event_id(), binary(), boolean()) -> data().
new(EventId, DataAsBinary, IsDataFinal) ->
    if
        IsDataFinal andalso size(DataAsBinary) < 4096 -> %% At 64, it is in shared space
            #data{
                type = raw,
                payload = DataAsBinary
            };
        true ->
            FileName = ?ROOT_STORAGE_DATA_DIR ++ binary_to_list(EventId),
            case IsDataFinal of
                true ->
                    ok = file:write_file(FileName, DataAsBinary);
                false ->
                    %% Take the lock of this file to write
                    FileObject = gen_server:call(?MODULE, {new_file_object, FileName}),
                    ok = file:pwrite(FileObject#file.fd, eof, DataAsBinary),
                    %% Release the lock for other writers, warn subscribers, unblock next writer
                    gen_server:cast(?MODULE, {set_file_size, FileName, DataAsBinary, IsDataFinal})
            end,
            #data{
                type = file,
                payload = FileName
            }
    end.

raw(DataObject) when DataObject#data.type =:= file ->
    <<>>;
raw(DataObject) ->
    DataObject#data.payload.

%% @todo: add ability to stop subscription as well
-spec to_binary(data(), spread_pid:spread_pid()) -> binary().
to_binary(DataObject, Pid) when DataObject#data.type =:= file ->
    case gen_server:call(?MODULE, {get_current_file_object, DataObject, {subscriber, Pid}}) of
        none ->
            {ok, Binary} = file:read_file(DataObject#data.payload),
            spread_pid:send({fin, <<>>}, Pid),
            Binary;
        FileObject ->
            case file:pread(FileObject#file.fd, bof, FileObject#file.size) of
                {ok, Binary} ->
                    Binary;
                Any ->
                    lager:error("Error reading file ~p: ~p", [DataObject#data.payload, Any]),
                    {error, Any}
            end
    end;
to_binary(DataObject, Pid) when DataObject#data.type =:= raw ->
    spread_pid:send({fin, <<>>}, Pid),
    DataObject#data.payload.

-spec update_with_more_data(data(), binary(), boolean()) -> ok | {error, any()}.
update_with_more_data(#data{payload = FileName} = DataObject, MoreDataAsBinary, IsDataFinal) ->
    case gen_server:call(?MODULE, {get_current_file_object, DataObject, {writer, self()}}) of
        none ->
            lager:error("Error, updating with data to non existing file object ~p", [DataObject#data.payload]),
            {error, no_data_object};
        FileObject ->
            case FileObject#file.state of
                ready ->
                    ok;
                writing ->
                    lager:debug("~p Writer being queued for ~p", [self(), FileName]),
                    receive
                        writer_go ->
                            ok
                    end
            end,
            lager:debug("~p writer can now proceed for ~p", [self(), FileName]),
            file:pwrite(FileObject#file.fd, eof, MoreDataAsBinary),
            gen_server:cast(?MODULE, {set_file_size, FileName, MoreDataAsBinary, IsDataFinal})
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    lager:debug("Init started"),
    filelib:ensure_dir(?ROOT_STORAGE_DATA_DIR ++ "1"),
    State = #state{},
    lager:debug("Init done"),
    erlang:send_after(60 * 1000, self(), {gc}),
    {ok, State}.


handle_call({new_file_object, FileName}, _From, State) ->
    {FileObject, NewState} = create_file(FileName, State),
    {reply, FileObject, NewState};
%% @todo: remove that global bottleneck, since we only need it PER DATAOBJECT, not globally
handle_call({get_current_file_object, DataObject, Client}, _From, State) ->
    #data{type = file, payload = FileName} = DataObject,
    case is_file_being_filled(FileName) of
        false ->
            {reply, none, State};
        FileObject ->
            case Client of
                {subscriber, Pid} ->
                    {NewFileObject, NewState} = add_subscriber(Pid, FileName, FileObject, State),
                    {reply, NewFileObject, NewState};
                {writer, Pid} ->
                    case FileObject#file.state of
                        ready ->
                            {_NewFileOject, NewState} = set_writing_state(FileName, State),
                            {reply, FileObject, NewState}; %% We send the OLD state
                        writing ->
                            lager:error("New concurrent writer for ~p", [FileName]),
                            {NewFileObject, NewState} = add_writer(Pid, FileName, FileObject, State),
                            {reply, NewFileObject, NewState}
                    end
            end
    end;
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({set_file_size, FileName, NewBinary, IsDataFinal}, State) ->
    {_, NewState} = set_file_size(FileName, NewBinary, IsDataFinal, State),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gc}, State) ->
    erlang:garbage_collect(self()),
    erlang:send_after(60 * 1000, self(), {gc}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    close_files(get_files(State)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

close_files([]) -> ok;
close_files([A | Rest]) when is_record(A, file) ->
    file:close(A#file.fd),
    close_files(Rest);
close_files([_ | Rest]) ->
    close_files(Rest).

get_files(_State) -> %% MUST be called from gen_server process!
    [Val || {_Key, Val} <- get()].

create_file(FileName, State) -> %% MUST be called from gen_server process!
    case get(FileName) of
        undefined ->
            lager:debug("Opening ~p", [FileName]),
            {ok, File} = file:open(FileName, [append, binary, read]), %% Don't use raw as long as we don't use the same thread
            {ok, FileInfo} = file:read_file_info(FileName),
            FileObject = #file{fd = File, size = FileInfo#file_info.size, state = writing},
            put(FileName, FileObject),
            {FileObject, State};
        _Val ->
            lager:error("FileName already there ~p", [FileName]),
            error
    end.

set_writing_state(FileName, State) ->
    case get(FileName) of
        undefined ->
            lager:error("FileName ~p not found", [FileName]),
            {error, State};
        FileObject ->
            NewFileObject = FileObject#file{state = writing},
            NewState = set_file(FileName, NewFileObject, State),
            {NewFileObject, NewState}
    end.

set_file_size(FileName, NewBinary, IsDataFinal, State) ->
    case get(FileName) of
        undefined ->
            lager:error("FileName ~p not found", [FileName]),
            {error, State};
        FileObject ->
            %% Update file size
            NewFileSize = FileObject#file.size + size(NewBinary),
            %% Warn all subscribers with the new binary directly, no need for them to read the file!
            Prefix = case IsDataFinal of
                true -> fin;
                false -> nofin
            end,
            send_to_all_subscribers({Prefix, NewBinary}, FileObject#file.subscribers),
            %% Proceed
            case IsDataFinal of
                true ->
                    %% Final chunk, do the cleanup
                    file:close(FileObject#file.fd),
                    erase(FileName),
                    {none, State};
                false ->
                    %% More chunks to come, freeing next writer if any
                    case FileObject#file.writers of
                        [] ->
                            %% No more writer, setting the new size
                            NewFileObject = FileObject#file{state = ready, size = NewFileSize},
                            NewState = set_file(FileName, NewFileObject, State),
                            {NewFileObject, NewState};
                        [Writer | Writers] ->
                            %% More writers, letting next one know he can proceed
                            Writer ! writer_go,
                            NewFileObject = FileObject#file{state = writing, size = NewFileSize, writers = Writers},
                            NewState = set_file(FileName, NewFileObject, State),
                            {NewFileObject, NewState}
                    end
            end
    end.

set_file(FileName, FileObject, State) ->
    put(FileName, FileObject),
    State.

is_file_being_filled(FileName) ->
    case get(FileName) of
        undefined ->
            false;
        Other ->
            Other
    end.

send_to_all_subscribers(_Payload, []) ->
    ok;
send_to_all_subscribers(Payload, [Sub | Subs]) ->
    spread_pid:send(Payload, Sub),
    send_to_all_subscribers(Payload, Subs).

add_subscriber(Pid, FileName, FileObject, State) ->
    NewFileObject = FileObject#file{subscribers = [Pid | FileObject#file.subscribers]},
    put(FileName, NewFileObject),
    {NewFileObject, State}.

add_writer(Pid, FileName, FileObject, State) ->
    NewFileObject = FileObject#file{writers = FileObject#file.writers ++ [Pid]},
    put(FileName, NewFileObject),
    {NewFileObject, State}.
