%%%-------------------------------------------------------------------
%% @doc spread public API
%% @end
%%%-------------------------------------------------------------------

-module(spread).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
-export([get/1, get/2]).
-export([post/2]).
%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    spread_event:init(),
    spread_sup:start_link().


%%--------------------------------------------------------------------
stop(_State) ->
    ok.

-spec get(list()) -> error | {integer(), binary(), binary()}.
get(Path) ->
    case get(Path, self()) of
        {Date, From, FirstChunk} ->
            {Date, From, loop_to_get(FirstChunk)};
        error ->
            error
    end.

-spec get(list(), pid()) -> error | {integer(), binary(), binary()}.
get(Path, Pid) ->
    case spread_topic_cache:get_latest(Path) of
        error ->
            error;
        {Date, Event} ->
            From = spread_event:from(Event),
            Data = spread_event:data(Event),
            FirstChunk = spread_data:to_binary(Data, Pid),
            {Date, From, FirstChunk}
    end.

-spec post(list(), binary()) -> {spread_event:event(), [any()]} | {error, any()}.
post(Path, Payload) ->
    spread_core:set_event(Path, atom_to_binary(node(), utf8), erlang:system_time(microsecond), Payload, true, []).

%%====================================================================
%% Internal functions
%%====================================================================
loop_to_get(Acc) ->
    receive
        {nofin, DataBinary} ->
            loop_to_get(<<Acc/binary, DataBinary/binary>>);
        {fin, DataBinary} ->
            <<Acc/binary, DataBinary/binary>>
    end.
