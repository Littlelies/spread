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
-export([subscribe/2]).
-export([subscribe_locally/2]).
-export([ensure_remote/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    spread_event:init(),
    spread_sup:start_link().


%%--------------------------------------------------------------------
stop(_State) ->
    ok.

-spec get(spread_topic:topic_name()) -> error | {integer(), binary(), binary()}.
get(Path) ->
    case get(Path, self()) of
        {Date, From, FirstChunk} ->
            {Date, From, loop_to_get(FirstChunk)};
        error ->
            error
    end.

-spec get(spread_topic:topic_name(), pid()) -> error | {integer(), binary(), binary()}.
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

-spec post(spread_topic:topic_name(), binary()) -> {spread_event:event(), [any()]} | {error, any()}.
post(Path, Payload) ->
    spread_core:set_event(Path, atom_to_binary(node(), utf8), erlang:system_time(microsecond), Payload, true, []).


-spec ensure_remote(atom()) -> {spread_event:event(), [any()]} | {error, any()}.
ensure_remote(NodeName) ->
    spread_gun:add_connection(NodeName).


-spec subscribe_locally(spread_topic:topic_name(), pid()) -> [{spread_topic:topic_name(), integer(), spread_event:event()}].
subscribe_locally(Path, Pid) ->
    autotree_app:subscribe(Path, 0, Pid).

-spec subscribe(spread_topic:topic_name(), pid()) -> [{spread_topic:topic_name(), integer(), spread_event:event()}].
subscribe(Path, Pid) ->
    %% Subscribe locally
    FirstSet = subscribe_locally(Path, Pid),
    %% Make sure we subscribe on remotes as well
    spread_gun:subscribe(spread_sub:new(Path, 0)),
    FirstSet.

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
