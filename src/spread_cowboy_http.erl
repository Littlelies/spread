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
    From = <<"testFRed">>,
    %case spread_cowboy:get_auth(Req) of
    %    error ->
    %        {ok, cowboy_req:reply(401, #{}, <<"Unauthenticated requests cannot POST.">>, Req), State};
    %    From ->
            process_post(Req, State, Path, From)
    %end
    ;
maybe_process(Req, State, <<"POST">>, _, false) ->
    {ok, cowboy_req:reply(400, #{}, <<"Missing body.">>, Req), State};
maybe_process(Req, State, <<"GET">>, Path, _) ->
    process_get(Req, State, Path);
maybe_process(Req, _, _, _, _) ->
    %% Method not allowed.
    cowboy_req:reply(405, Req).


%% Processing GET requests.
%% We send the body by chunks as they arrive
process_get(Req, State, Path) ->
    lager:info("~p GET ~p", [self(), Path]),

    %% @todo: check ETag in order to send 304
    case spread:get(Path, self()) of
        error ->
            lager:debug("~p Got error", [Path]),
            {ok, cowboy_req:reply(404, #{}, <<"Not found">>, Req), State};
        {Date, From, FirstChunk} ->
            Req1 = cowboy_req:stream_reply(200, #{
                <<"Content-Type">> => <<"application/octet-stream">>,
                <<"From">> => From,
                <<"ETag">> => <<"\"", (integer_to_binary(Date))/binary, "\"">>
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
    Prs = get_prs(Req0),
    lager:info("~p POST From ~p, Date ~p: ~p", [self(), From, Date, Path]),
    case cowboy_req:read_body(Req0, #{length => 64}) of
        {ok, Data, Req} ->
            lager:info("Got unique chunk, creating event"),
            {Event, Out} = spread_core:set_event(Path, From, Date, Data, true, Prs);
        {more, Data, Req} ->
            lager:info("Got first chunk, creating event ~p", [size(Data)]),
            {Event, Out} = spread_core:set_event(Path, From, Date, Data, false, Prs),
            read_body(Req, Event)
    end,
    lager:info("Upload done"),
    {ok, cowboy_req:reply(200, #{
        <<"content-type">> => <<"text/plain; charset=utf-8">>
    }, format_out(Event, Out), Req), State}.

read_body(Req0, Event) ->
    case cowboy_req:read_body(Req0, #{length => 256000}) of
        {ok, Data, _Req} ->
            lager:info("Last Chunk ~p", [size(Data)]),
            spread_core:add_data_to_event(Event, Data, true);
        {more, Data, Req} ->
            lager:info("Chunk ~p", [size(Data)]),
            spread_core:add_data_to_event(Event, Data, false),
            read_body(Req, Event)
    end.

get_date(Req) ->
    case cowboy_req:header(<<"etag">>, Req) of
        undefined ->
            erlang:system_time(microsecond);
        RawEtag ->
            [_, Etag, _] = binary:split(RawEtag, <<"\"">>, [global]),
            binary_to_integer(Etag)
    end.

get_prs(Req) ->
    QsVals = cowboy_req:parse_qs(Req),
    case lists:keyfind(<<"prs">>, 1, QsVals) of
        false ->
            [];
        RawPrs ->
            parse_raw_prs(RawPrs)
    end.

parse_raw_prs(_) ->
    [].

format_out(Event, Out) ->
    <<"{\"", (spread_event:id(Event))/binary, "\":", (format_outs(Out))/binary, "}">>.

format_outs([A | Rest]) when is_atom(A) ->
    format_outs(Rest, <<"[", (atom_to_binary(A, utf8))/binary>>);
format_outs([A | Rest]) when is_list(A) ->
    format_outs(Rest, <<"[\"local\"">>).

format_outs([], Acc) ->
    <<Acc/binary, "]">>;
format_outs([A | Rest], Acc) ->
    format_outs(Rest, <<Acc/binary , ",\"", (atom_to_binary(A, utf8))/binary, "\"">>).