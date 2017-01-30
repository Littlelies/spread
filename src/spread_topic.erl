-module(spread_topic).

-export([
    new/1,
    id/1,
    name/1,
    name_as_binary/1
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

-spec name_as_binary(topic()) -> binary().
name_as_binary(Topic) ->
    spread_utils:binary_join(Topic#topic.name, <<"/">>).