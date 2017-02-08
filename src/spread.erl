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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

-spec post(spread_topic:topic_name(), binary()) -> {existing | new, spread_event:event(), [any()]} | {error, any()}.
post(Path, Payload) ->
    spread_core:set_event(Path, atom_to_binary(node(), utf8), erlang:system_time(microsecond), Payload, true, []).


-spec ensure_remote(atom()) -> {existing | new, spread_event:event(), [any()]} | {error, any()}.
ensure_remote(NodeName) ->
    spread_gun:add_connection(NodeName).

-spec subscribe_locally(spread_topic:topic_name(), pid()) -> [{spread_topic:topic_name(), integer(), spread_event:event()}].
subscribe_locally(Path, Pid) ->
    spread_autotree:subscribe(Path, 0, Pid).

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

-ifdef(TEST).
spread_test() ->
    os:cmd("rm -rf storage"),
    application:start(syntax_tools),
    application:start(compiler),
    application:start(goldrush),
    application:start(lager),
    application:start(cowlib),
    application:start(ranch),
    application:start(gun),
    application:start(cowboy),
    application:start(autotree),
    application:start(jsx),
    application:start(base64url),
    application:start(jwt),
    application:start(spread),
    spread:ensure_remote('localhost:8080'),

    lager:info("TEST: We can subscribe to a path where there is nothing yet"),
    spread:subscribe([<<"test">>], self()),

    lager:info("TEST: We can post new data and get the amount of warned subscribers for each sub path"),
    SmallPayload =  <<"test small payload">>,
    SmallTopic = [<<"test">>, <<"test1">>],
    {new, _Event, List} = spread:post(SmallTopic, SmallPayload),
    ?assertEqual(List, [[{[],0}, {[<<"test">>], 1}, {[<<"test">>, <<"test1">>], 0}]]),
    assert_update_received(SmallTopic),

    lager:info("TEST: We can get stored data with info"),
    {_Time, From, Pay} = spread:get(SmallTopic),
    ?assertEqual(
        From,
        atom_to_binary(node(), utf8)
    ),
    ?assertEqual(
        Pay,
        SmallPayload
    ),

    lager:info("TEST: We can post new small data via HTTP"),
    os:cmd("cd apps/spread/tests && ./post_small_http.sh"),
    assert_update_received([<<"test">>]),

    lager:info("TEST: We can post new big data via HTTP"),
    os:cmd("cd apps/spread/tests && ./post_big_http.sh"),
    assert_update_received([<<"test">>, <<"image">>]).

assert_update_received(Topic) ->
    receive
        {update, ReceivedTopic, _Timestamp, _Event} ->
            ?assertEqual(
                ReceivedTopic,
                Topic
            );
        Any ->
            ?assertEqual(
                Any,
                right_update_message
            )
    after 0 ->
        ?assertEqual(
            no_message_received,
            message_received
        )
    end.
    
-endif.