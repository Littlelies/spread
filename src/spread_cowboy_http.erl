%%%-------------------------------------------------------------------
%% @doc spread cowboy HTTP interface. Used to POST events, GET binaries
%% @end
%%%-------------------------------------------------------------------
-module(spread_cowboy_http).

-export([init/2]).
-export([terminate/3]).

%%====================================================================
%% API functions
%%====================================================================

init(Req, State) ->
    Path = cowboy_req:path_info(Req),
    Method = cowboy_req:method(Req),
    HasBody = cowboy_req:has_body(Req),
    maybe_process(Req, State, Method, Path, HasBody).

terminate(_Reason, _Req, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

maybe_process(Req, State, <<"POST">>, Path, true) ->
    case spread_cowboy:get_auth(Req) of
        error ->
            {ok, cowboy_req:reply(401, #{}, <<"Unauthenticated requests cannot POST.">>, Req), State};
        {error, Reason} ->
            lager:error("Error auth ~p", [Reason]),
            {ok, cowboy_req:reply(401, #{}, <<"Unauthenticated requests cannot POST.">>, Req), State};
        From ->
            process_post(Req, State, Path, From)
    end;
maybe_process(Req, State, <<"POST">>, _, false) ->
    {ok, cowboy_req:reply(400, #{}, <<"Missing body.">>, Req), State};
maybe_process(Req, State, <<"GET">>, Path, _) ->
    process_get(Req, State, Path);
maybe_process(Req, State, <<"OPTIONS">>, _, _) ->
    {ok, cowboy_req:reply(200, #{
        <<"content-type">> => <<"text/plain; charset=utf-8">>,
        <<"access-control-allow-origin">> => <<"*">>,
        <<"access-control-allow-headers">> => <<"authorization">>,
        <<"access-control-allow-method">> => <<"POST">>
    } , <<>>, Req), State};
maybe_process(Req, State, _, _, _) ->
    %% Method not allowed.
    {ok, cowboy_req:reply(405, #{}, <<"Method not allowed.">>, Req), State}.


%% Processing GET requests.
%% We send the body by chunks as they arrive
process_get(Req, State, Path) ->
    lager:debug("~p GET ~p", [self(), Path]),

    %% @todo: check ETag in order to send 304
    case spread:get(Path, self()) of
        error ->
            lager:debug("~p Got error", [Path]),
            {ok, cowboy_req:reply(404, #{}, <<"Not found">>, Req), State};
        {Date, From, FirstChunk} ->
            Req1 = cowboy_req:stream_reply(200, #{
                <<"content-type">> => <<"application/octet-stream">>,
                <<"access-control-allow-origin">> => <<"*">>,
                <<"from">> => From,
                <<"x-spread-etag">> => <<"\"", (integer_to_binary(Date))/binary, "\"">>
            }, Req),
            cowboy_req:stream_body(FirstChunk, nofin, Req1),
            %% @todo: respect API and send the loop right away
            send_next_chunks(Req1, State)
    end.

send_next_chunks(Req, State) ->
    receive
        {nofin, DataBinary} ->
            cowboy_req:stream_body(DataBinary, nofin, Req),
            send_next_chunks(Req, State);
        {fin, DataBinary} ->
            cowboy_req:stream_body(DataBinary, fin, Req),
            {ok, Req, State};
        Any ->
            lager:error("Unexpected data ~p", [Any])
    end.


%% Processing POST requests.
%% We upload the body by chunks, updating all subscribers at the same time
process_post(Req0, State, Path, From) ->
    Date = get_date(Req0),
    lager:debug("~p POST From ~p, Date ~p: ~p", [self(), From, Date, Path]),
    Answer = case cowboy_req:read_body(Req0, #{length => 64}) of
        {ok, Data, Req} ->
            lager:debug("Got unique chunk, creating event"),
            {_IsNew, Event, Out, _PreviousOrError} = spread_core:set_event(Path, From, Date, Data, true, false),
            true;
        {more, Data, Req} ->
            lager:debug("Got first chunk, creating event ~p", [size(Data)]),
            {IsNew, Event, Out, _PreviousOrError} = spread_core:set_event(Path, From, Date, Data, false, false),
            case IsNew of
                new ->
                    read_body(Req, Event),
                    true;
                existing ->
                    false
            end
    end,
    lager:debug("Upload done"),
    case Answer of
        true ->
            {ok, cowboy_req:reply(200, #{
                <<"content-type">> => <<"text/plain; charset=utf-8">>,
                <<"access-control-allow-origin">> => <<"*">>
            }, format_out(Event, Out), Req), State};
        false ->
            cowboy_req:reply(409, Req)
    end.

read_body(Req0, Event) ->
    case cowboy_req:read_body(Req0, #{length => 256000}) of
        {ok, Data, _Req} ->
            lager:debug("Last Chunk ~p", [size(Data)]),
            spread_core:add_data_to_event(Event, Data, true);
        {more, Data, Req} ->
            lager:debug("Chunk ~p", [size(Data)]),
            spread_core:add_data_to_event(Event, Data, false),
            read_body(Req, Event)
    end.

get_date(Req) ->
    case cowboy_req:header(<<"x-spread-etag">>, Req) of
        undefined ->
            erlang:system_time(microsecond);
        RawEtag ->
            [_, Etag, _] = binary:split(RawEtag, <<"\"">>, [global]),
            binary_to_integer(Etag)
    end.

format_out(Event, Out) ->
    <<"{\"", (spread_event:id(Event))/binary, "\":", (format_out(Out))/binary, "}">>.

format_out({too_late, _Event}) ->
    <<"{\"status\": \"too_late\"}">>;
format_out({Iteration, Propagations, _Event}) ->
    <<"{\"status\": \"ok\", \"iteration\": ", (integer_to_binary(Iteration))/binary, ", \"propagation\": ", (format_propagations(Propagations))/binary, "}">>.

format_propagations(Propagations) ->
    <<"[", (format_propagations(Propagations, true))/binary, "]">>.

format_propagations([], _) ->
    <<>>;
format_propagations([A | Rest], true) ->
    <<(format_propagation(A))/binary, (format_propagations(Rest, false))/binary>>;
format_propagations([A | Rest], false) ->
    <<", ", (format_propagation(A))/binary, (format_propagations(Rest, false))/binary>>.

format_propagation({PathAsList, Count}) ->
    <<(format_path_as_list(PathAsList))/binary, ": ", (integer_to_binary(Count))/binary>>.

format_path_as_list(Path) ->
    spread_utils:binary_join(Path).
