-module(spread_gun_connection).

-behaviour(gen_server).

-export([start_link/1]).

%% @todo: manage timeouts for each stream!

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {target, connpid, streams = [], state = down}).
-record(stream, {ref, ownerpid}).

start_link(Target) ->
    gen_server:start_link(?MODULE, [Target], []).

init([Target]) ->
    %% Subscribe to the list of servers to be linked to
    {ok, ConnPid} = gun:open(atom_to_list(Target), 443),
    {ok, #state{connpid = ConnPid, target = Target}}.

handle_call({send, Event, OwnerPid}, _From, State) ->
    StreamRef = gun:post(State#state.connpid, spread_topic:name_as_binary(spread_event:topic(Event)),
        [
            {<<"From">>, spread_event:from(Event)},
            {<<"Etag">>, <<"\"", (integer_to_binary(spread_event:date(Event)))/binary, "\"">>},
            {<<"Content-Type">>, <<"application/octet-stream">>}
        ]),
    case spread_data:to_binary(spread_event:data(Event), {self(), {data_for_stream, StreamRef}}) of
        <<>> ->
            lager:info("Empty binary, not sending");
        Binary ->
            gun:data(State#state.connpid, StreamRef, nofin, Binary)
    end,
    {reply, ok, add_stream(#stream{ref = StreamRef, ownerpid = OwnerPid}, State)};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gun_up, _ConnPid, http2}, State) ->
    lager:info("[~p] Connection is up", [State#state.target]),
    {noreply, State#state{state = up}};
handle_info({gun_down, _ConnPid, _Protocol, _Reason, _Processed, _NotProcessed}, State) ->
    lager:info("[~p] Connection is down", [State#state.target]),
    {noreply, State#state{state = down}};
handle_info({gun_response, _ConnPid, StreamRef, fin, _Status, Headers}, State) ->
    lager:debug("[~p] No data for ~p, headers are ~p", [State#state.target, StreamRef, Headers]),
    {noreply, remove_stream(StreamRef, State)};
handle_info({gun_response, _ConnPid, StreamRef, nofin, _Status, Headers}, State) ->
    lager:debug("[~p] Got headers for ~p: ~p", [State#state.target, StreamRef, Headers]),
    {noreply, State};
handle_info({gun_data, _ConnPid, StreamRef, nofin, Data}, State) ->
    lager:debug("[~p] Got partial data for ~p: ~p", [State#state.target, StreamRef, Data]),
    {noreply, State};
handle_info({gun_data, _ConnPid, StreamRef, fin, Data}, State) ->
    lager:debug("[~p] Got final data for ~p: ~p", [State#state.target, StreamRef, Data]),
    {noreply, remove_stream(StreamRef, State)};
handle_info({{data_for_stream, StreamRef}, {Prefix, Binary}}, State) ->
    gun:data(State#state.connpid, StreamRef, Prefix, Binary),
    NewState = case Prefix of
        nofin ->
            State;
        fin ->
            lager:info("Upload to connection is done"),
            remove_stream(StreamRef, State)
    end,
    {noreply, NewState};
handle_info({'DOWN', _MRef, process, _ConnPid, Reason}, State) ->
    lager:error("[~p] Gun connection is dead because of a gun bug: ~p", [State#state.target, Reason]),
    {stop, Reason, ok, State};
handle_info(stop, State) ->
    {stop, normal, State};
handle_info({stop, Reason}, State) ->
    {stop, Reason, State};
handle_info({gun_error, _MRef, StreamRef, Any}, State) ->
    % {badstate,"The stream has already been closed."}, {badstate,"The stream cannot be found."}
    %% @todo: remove the stream
    %% @todo stop our subscription to spread_data:to_binary
    lager:info("Stream ~p says: ~p", [StreamRef, Any]),
    {noreply, State};
handle_info(_Info, State) ->
    lager:info("Unknown info ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, State) ->
    gun:shutdown(State#state.connpid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions
add_stream(Stream, State) ->
    State#state{streams = [Stream | State#state.streams]}.

remove_stream(StreamRef, State) ->
    Streams = State#state.streams,
    NewStreams = lists:keydelete(StreamRef, 2, Streams),
    State#state{streams = NewStreams}.
