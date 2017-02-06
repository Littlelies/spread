-module(spread_sub).

-export([
    new/2,
    path/1,
    timestamp/1
]).

-record(sub, {
    path,
    timestamp
}).
-type sub() :: #sub{}.

-export_type([sub/0]).

-spec new(list(), integer()) -> sub().
new(Path, Timestamp) ->
    #sub{path = Path, timestamp = Timestamp}.

-spec path(sub()) -> list().
path(Sub) ->
    Sub#sub.path.

-spec timestamp(sub()) -> integer().
timestamp(Sub) ->
    Sub#sub.timestamp.
