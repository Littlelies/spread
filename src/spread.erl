%%%-------------------------------------------------------------------
%% @doc spread public API
%% @end
%%%-------------------------------------------------------------------

-module(spread).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
-export([get/1, get/2]).
-export([maybe_post/2, post/2]).
-export([subscribe/2]).
-export([subscribe_locally/2]).
-export([ensure_remote/1, ensure_remote/2]).

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

%% Get in local cache the spread_topic. If you expect data from remote, please make sure the node is subscribed to it or to one of the ancestors
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
        {Iteration, Event} ->
            From = spread_event:from(Event),
            Data = spread_event:data(Event),
            FirstChunk = spread_data:to_binary(Data, Pid),
            {Iteration, From, FirstChunk}
    end.

-spec post(spread_topic:topic_name(), binary()) -> {existing | new, spread_event:event(), {too_late, spread_event:event()} | {autotree_app:iteration(), [{[any()], integer()}], spread_event:event() | error}, spread_event:event() | error} | {error, any()}.
post(Path, Payload) ->
    spread_core:set_event(Path, atom_to_binary(node(), utf8), erlang:system_time(microsecond), Payload, true, false).

-spec maybe_post(spread_topic:topic_name(), binary()) -> {existing | new, spread_event:event(), {too_late, spread_event:event()} | {autotree_app:iteration(), [{[any()], integer()}], spread_event:event() | error}, spread_event:event() | error} | {error, any()}.
maybe_post(Path, Payload) ->
    spread_core:set_event(Path, atom_to_binary(node(), utf8), erlang:system_time(microsecond), Payload, true, true).

-spec ensure_remote(atom()) -> {existing | new, spread_event:event(), [any()], spread_event:event() | error} | {error, any()}.
ensure_remote(NodeName) ->
    ensure_remote(NodeName, <<"Bearer Anonymous">>).

-spec ensure_remote(atom(), binary()) -> {existing | new, spread_event:event(), [any()], spread_event:event() | error} | {error, any()}.
ensure_remote(NodeName, Auth) ->
    spread_gun:add_connection(NodeName, Auth).

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
    %os:cmd("rm -rf storage"), % run once, then comment and run again
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

    lager:debug("TEST: We can subscribe to a path where there is nothing yet"),
    spread:subscribe([<<"test">>], self()),

    lager:debug("TEST: We can post new data and get the amount of warned subscribers for each sub path"),
    SmallPayload =  <<"test small payload but so small just to make sure we create a new file for that small payload no?">>,
    SmallTopic = [<<"test">>, <<"test1">>],
    {new, Event, {It, List, _}, _} = spread:post(SmallTopic, SmallPayload),
    lager:debug("List ~p, Iteration ~p", [List, It]),
    ?assertEqual([{[],0}, {[<<"test">>], 1}, {[<<"test">>, <<"test1">>], 0}], List),
    assert_update_received(SmallTopic),

    lager:debug("TEST: We can fail to update if data is already here"),
    It2 = It + 1,
    {new, _Event2, {It2, [], Event}, Event} = spread:maybe_post(SmallTopic, <<"whatever, won't be taken into account">>),

    lager:debug("TEST: We get error if nothing was here before"),
    It3 = It + 2,
    {new, _Event3, {It3, List2, error}, error} = spread:post([<<"test">>, <<"test_bogus">>], <<"something">>),
    lager:debug("LIST2 ~p", [List2]),
    ?assertEqual([{[],0}, {[<<"test">>], 1}, {[<<"test">>, <<"test_bogus">>], 0}], List2),
    assert_update_received([<<"test">>, <<"test_bogus">>]),

    lager:debug("TEST: We can get stored data with info"),
    {_Time, From, Pay} = spread:get(SmallTopic),
    ?assertEqual(
        From,
        atom_to_binary(node(), utf8)
    ),
    ?assertEqual(
        Pay,
        SmallPayload
    ),
    ?assertEqual(os:cmd("cd apps/spread/tests && ./get.sh"), binary_to_list(SmallPayload)),

    lager:debug("TEST: We can post new small data via HTTP"),
    lager:debug("Command: ~p", [os:cmd("cd apps/spread/tests && ./post_small_http.sh")]),
    assert_update_received([<<"test">>]),

    lager:debug("TEST: We can post new big data via HTTP"),
    os:cmd("cd apps/spread/tests && ./post_big_http.sh &"),
    os:cmd("cd apps/spread/tests && ./post_big_http.sh"),
    assert_update_received([<<"test">>, <<"image">>]),
    assert_update_received([<<"test">>, <<"image">>]),

    lager:debug("TEST: Not authenticated requests are rejected"),
    ?assertEqual(os:cmd("cd apps/spread/tests && ./post_no_auth.sh"), "401"),

    lager:debug("TEST: POST without body are rejected"),
    ?assertEqual(os:cmd("cd apps/spread/tests && ./post_no_body.sh"), "400"),

    lager:debug("TEST: GET of unknown resources end up in 404"),
    ?assertEqual(os:cmd("cd apps/spread/tests && ./get_404.sh"), "404"),

    lager:debug("TEST: Backup data"),
    spread_topic_cache ! {gc},
    timer:sleep(200),
    ok
%    ?assertEqual(
%        file:
%    )
.



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