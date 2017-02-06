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

-record(state, {
    target,
    connpid,
    streams = [],
    state = down,
    subs = [],
    partial = <<>>
}).
% -record(stream, {
%     ref,
%     ownerpid
% }).
start_link(Target) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Target], []).

init([Target]) ->
    {ok, ConnPid} = gun:open(atom_to_list(Target), 443),
    Subs = spread_gun_subscription_manager:get_subs(),
    {ok, #state{connpid = ConnPid, target = Target, subs = Subs}}.


handle_call({subscribe, Sub}, _From, State) ->
    {reply, ok, add_subscriptions([Sub], State)};
% handle_call({send, Event, OwnerPid}, _From, State) ->
%     StreamRef = gun:post(State#state.connpid, spread_topic:name_as_binary(spread_event:topic(Event)),
%         [
%             {<<"From">>, spread_event:from(Event)},
%             {<<"Etag">>, <<"\"", (integer_to_binary(spread_event:date(Event)))/binary, "\"">>},
%             {<<"Content-Type">>, <<"application/octet-stream">>}
%         ]),
%     case spread_data:to_binary(spread_event:data(Event), {self(), {data_for_stream, StreamRef}}) of
%         <<>> ->
%             lager:info("Empty binary, not sending");
%         Binary ->
%             gun:data(State#state.connpid, StreamRef, nofin, Binary)
%     end,
%     {reply, ok, add_stream(#stream{ref = StreamRef, ownerpid = OwnerPid}, State)};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gun_up, _ConnPid, http2}, State) ->
    lager:info("[~p] Connection is up", [State#state.target]),
    %% Re open all streams
    NewState = add_subscriptions(State#state.subs, State),
    {noreply, NewState#state{state = up}};
handle_info({gun_down, _ConnPid, _Protocol, _Reason, _Processed, _NotProcessed}, State) ->
    lager:info("[~p] Connection is down", [State#state.target]),
    {noreply, State#state{state = down}};
handle_info({gun_response, _ConnPid, StreamRef, fin, _Status, Headers}, State) ->
    lager:debug("[~p] No data for ~p, headers are ~p", [State#state.target, StreamRef, Headers]),
    {noreply, remove_stream(StreamRef, State)};
handle_info({gun_response, _ConnPid, StreamRef, nofin, _Status, Headers}, State) ->
    lager:debug("[~p] Got headers for ~p: ~p", [State#state.target, StreamRef, Headers]),
    NewState = manage_data(Data, StreamRef, State),    
    {noreply, NewState};
handle_info({gun_data, _ConnPid, StreamRef, nofin, Data}, State) ->
    lager:debug("[~p] Got partial data for ~p: ~p", [State#state.target, StreamRef, Data]),
    NewState = manage_data(Data, StreamRef, State),
    {noreply, NewState};
handle_info({gun_data, _ConnPid, StreamRef, fin, Data}, State) ->
    lager:debug("[~p] Got final data for ~p: ~p", [State#state.target, StreamRef, Data]),
    NewState = manage_data(Data, StreamRef, State),
    {noreply, remove_stream(StreamRef, NewState)};
% handle_info({{data_for_stream, StreamRef}, {Prefix, Binary}}, State) ->
%     gun:data(State#state.connpid, StreamRef, Prefix, Binary),
%     NewState = case Prefix of
%         nofin ->
%             State;
%         fin ->
%             lager:info("Upload to connection is done"),
%             remove_stream(StreamRef, State)
%     end,
%     {noreply, NewState};
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

%%====================================================================
%% Internal functions
%%====================================================================

add_subscriptions([Sub | Subs], State) ->
    Stream = gun:get(State#state.connpid,
        <<"/sse/", (spread_topic:name_as_binary(spread_sub:path(Sub)))/binary>>,
        [
            {<<"last-event-id">>, integer_to_list(spread_sub:timestamp(Sub))}
        ]),
    add_subscriptions(Subs, add_stream(Stream, State)).

add_stream(Stream, State) ->
    State#state{streams = [Stream | State#state.streams]}.

remove_stream(StreamRef, State) ->
    Streams = State#state.streams,
    NewStreams = lists:keydelete(StreamRef, 2, Streams),
    State#state{streams = NewStreams}.

manage_data(Data, _StreamRef, State) ->
    {Partial, Updates} = spread_cowboy_sse:parse_updates(<<(State#state.parial)/binary, Data/binary>>),
    %% Manage updates internally
    publish_internally_updates(Updates),
    %% Loop with partial
    State#state{partial = Partial}.

publish_internally_updates([]) ->
    ok;
publish_internally_updates([{Path, Date, Data} | Updates]) ->
    spread_core:set_event(Path, From, Date, Data, true, Prs),
    publish_internally_updates(Updates).

