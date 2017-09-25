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
    state = init,
    subs = [],
    subs_streams = [],
    partial = <<>>,
    iteration
}).
-record(stream, {
     gun_stream,
     event
}).
-record(sub_stream, {
    gun_stream,
    sub
}).
-type state() :: #state{}.

start_link(Target) ->
    gen_server:start(?MODULE, [Target], []).

init([Target]) ->
    self() ! {init, Target},
    {ok, #state{target = Target}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({subscribe, Sub}, State) when State#state.state =/= init ->
    %% @todo: avoid duplicates??
    {noreply, add_subscriptions([Sub], State#state{subs = [Sub | State#state.subs]})};
handle_cast({load_binary_event, Event}, State) ->
    {noreply, add_binary_stream(Event, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({init, Target}, State) ->
    {Host, Port} = get_host_and_port(Target),
    case gun:open(Host, Port, #{protocols => [http2]}) of
        {ok, ConnPid} ->
            _MRef = monitor(process, ConnPid),
            Subs = spread_gun_subscription_manager:get_subs(),
            {noreply, State#state{connpid = ConnPid, subs = Subs, state = down}};
        {error, Reason} ->
            lager:error("FAILED TO CONNECT TO ~p, RETRYING", [Reason]),
            timer:send_after(2000, {init, Target}),
            {noreply, State}
    end;
handle_info({gun_up, _ConnPid, HTTPVersion}, State) ->
    lager:info("[~p] Connection is up using ~p", [State#state.target, HTTPVersion]),
    %% Re open all streams
    NewState = add_subscriptions(State#state.subs, State),
    {noreply, NewState#state{state = up}};
handle_info({gun_down, _ConnPid, _Protocol, _Reason, _Processed, _NotProcessed}, State) ->
    lager:info("[~p] Connection is down", [State#state.target]),
    {noreply, State#state{state = down}};
handle_info({gun_response, _ConnPid, StreamRef, fin, _Status, Headers}, State) ->
    lager:info("[~p] No data for ~p, headers are ~p", [State#state.target, StreamRef, Headers]),
    {_, NewState} = remove_stream(StreamRef, State),
    {noreply, NewState};
handle_info({gun_response, _ConnPid, StreamRef, nofin, _Status, Headers}, State) ->
    lager:info("[~p] Got headers for ~p: ~p", [State#state.target, StreamRef, Headers]),
    %NewState = manage_data(Headers, StreamRef, false, State),    
    {noreply, State};
handle_info({gun_data, _ConnPid, StreamRef, nofin, Data}, State) ->
    lager:info("[~p] Got partial data for ~p: ~p", [State#state.target, StreamRef, Data]),
    NewState = manage_data(Data, StreamRef, false, State),
    {noreply, NewState};
handle_info({gun_data, _ConnPid, StreamRef, fin, Data}, State) ->
    lager:info("[~p] Got final data for ~p: ~p", [State#state.target, StreamRef, Data]),
    NewState1 = manage_data(Data, StreamRef, true, State),
    {OldSub, NewState2} = remove_stream(StreamRef, NewState1),
    NewState = case OldSub of
        not_a_sub_stream ->
            NewState2;
        _ ->
            lager:info("Reconnect sub ~p", [OldSub]),
            add_subscriptions([OldSub], NewState2)
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
    lager:info("[spread_gun_connection] Unknown info ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, State) when State#state.state =/= init ->
    gun:shutdown(State#state.connpid);
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
add_subscriptions([], State) ->
    State;
add_subscriptions([Sub | Subs], State) ->
    lager:info("Adding subscription ~p to ~p", [Sub, State]),
    % @todo: what happens when we do that and connection is down???
    SubPath = <<"/sse/", (spread_topic:name_as_binary(spread_sub:path(Sub)))/binary>>,
    lager:info("Path is ~p", [SubPath]),
    Stream = gun:get(State#state.connpid,
        SubPath,
        [
            {<<"last-event-id">>, integer_to_list(spread_sub:timestamp(Sub))}
        ]),
    add_subscriptions(Subs, add_subs_stream(Stream, Sub, State)).

add_binary_stream(Event, State) ->
    Stream = gun:get(State#state.connpid,
        <<"/raw/", (spread_topic:name_as_binary(spread_event:topic(Event)))/binary>>),
    add_stream(Stream, Event, State).

add_stream(Stream, Event, State) ->
    State#state{streams = [#stream{gun_stream = Stream, event = Event} | State#state.streams]}.

add_subs_stream(Stream, Sub, State) ->
    State#state{subs_streams = [#sub_stream{gun_stream = Stream, sub = Sub} | State#state.subs_streams]}.


remove_stream(StreamRef, State) ->
    Streams = State#state.streams,
    NewStreams = lists:keydelete(StreamRef, 2, Streams),

    SubStreams = State#state.subs_streams,
    {OldSubStream, NewSubStreams} = case lists:keytake(StreamRef, 2, SubStreams) of
        {value, Tuple, NewTuples} ->
            {Tuple#sub_stream.sub, NewTuples};
        false ->
            {not_a_sub_stream, SubStreams}
    end,

    {OldSubStream, State#state{streams = NewStreams, subs_streams = NewSubStreams}}.

-spec manage_data(binary(), any(), boolean(), state()) -> state().
manage_data(Data, StreamRef, IsFin, State) ->
    lager:info("Manage ~p in ~p", [StreamRef, State#state.streams]),
    case lists:keyfind(StreamRef, 2, State#state.streams) of
        false ->
            Self = self(),
            {Partial, Iteration, _Updates} = spread_autotree:parse_updates_and_broadcast(
                <<(State#state.partial)/binary, Data/binary>>,
                fun(Event) ->
                    gen_server:cast(Self, {load_binary_event, Event})
                end),
            %% Loop with partial
            State#state{partial = Partial, iteration = Iteration};
        Stream ->
            %% Send this data
            spread_core:add_data_to_event(Stream#stream.event, Data, IsFin),
            State
    end.

get_host_and_port(Target) ->
    Url = atom_to_list(Target),
    case re:split(Url, ":", [{return, list}]) of
        [Host, PortAsList] ->
            Port = list_to_integer(PortAsList);
        [Host] ->
            Port = 8080
    end,
    {Host, Port}.
