-module(spread_topic).

-export([
    new/1,
    id/1,
    name/1,
    name_as_binary/1,
    binary_to_name/1
    ]).
-type topic_name() :: [binary()].

-record(topic, {
    id :: binary(),
    name :: topic_name()
}).
-type topic() :: #topic{}.

-export_type([topic/0, topic_name/0]).

-spec new([binary()]) -> {binary(), topic()}.
new(TopicName) ->
    TopicId = spread_utils:hash(TopicName),
    {TopicId, #topic{name = TopicName, id = TopicId}}.

-spec id(topic()) -> binary().
id(Topic) ->
    Topic#topic.id.

-spec name(topic()) -> topic_name().
name(Topic) ->
    Topic#topic.name.

-spec name_as_binary(topic() | topic_name()) -> binary().
name_as_binary(Topic) when is_record(Topic, topic) ->
    spread_utils:binary_join(Topic#topic.name);
name_as_binary(TopicName) ->
    spread_utils:binary_join(TopicName).

binary_to_name(Binary) ->
    binary:split(Binary, <<"/">>, [global]).
