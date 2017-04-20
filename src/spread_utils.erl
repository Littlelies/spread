-module(spread_utils).
-export([
    write_terms/2,
    read_terms/1,
    hash/1,
    microseconds_to_rfc7231/1,
    binary_join/1, binary_join/2,
    escape/1
    ]).

write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

read_terms(FileName) ->
    {ok, Terms} = file:consult(FileName),
    Terms.

-spec hash(any()) -> binary().
hash(Term) ->
    <<Hash:160>> = crypto:hash(sha,term_to_binary(Term)),
    list_to_binary(io_lib:format("~40.16.0b", [Hash])).

microseconds_to_rfc7231(Microseconds) ->
    cow_date:rfc7231(calendar:gregorian_seconds_to_datetime(62167219200 + (Microseconds div 1000000))).

-spec binary_join([binary()], binary()) -> binary().
binary_join([], _Sep) ->
  <<>>;
binary_join([Part], _Sep) ->
  Part;
binary_join([Head|Tail], Sep) ->
  lists:foldl(fun (Value, Acc) -> <<Acc/binary, Sep/binary, Value/binary>> end, Head, Tail).

-spec binary_join([binary()]) -> binary().
binary_join([]) ->
  <<>>;
binary_join([Part]) ->
  Part;
binary_join([Head | Tail]) ->
  lists:foldl(fun (Value, Acc) -> <<Acc/binary, "/", (escape(Value))/binary>> end, Head, Tail).

-spec escape(binary()) -> binary().
escape(Binary) ->
    list_to_binary(http_uri:encode(binary_to_list(Binary))).
