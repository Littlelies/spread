-module(spread_jwt_auth).

-export([auth/1]).
-export([init/2, terminate/3]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec auth(binary()) -> binary() | {error, atom()}.
auth(Authorization) ->
    case {application:get_env(spread, jwt_key), application:get_env(spread, jwt_iss)} of
        {undefined, undefined} ->
            <<"anonymous">>;
        {{ok, Key}, {ok, Iss}} ->
            case jwt:decode(Authorization, Key) of
                {ok, Claims} ->
                    lager:info("Claims are ~p", [Claims]),
                    case catch maps:get(<<"iss">>, Claims) of
                        Iss ->
                            try maps:get(<<"uid">>, Claims) of
                                Uid -> Uid
                            catch
                                _:_ ->
                                    {error, no_uid}
                            end;
                        _ ->
                            {error, bad_issuer}
                    end;
                {error, Any} ->
                    {error, Any}
            end
    end.

generate_token() ->
    Random = integer_to_binary(erlang:system_time(microsecond)),
    case {application:get_env(spread, jwt_key), application:get_env(spread, jwt_iss)} of
        {undefined, undefined} ->
            <<"anonymous">>;
        {{ok, Key}, {ok, Iss}} ->
            Claims = [
                {<<"iss">>, Iss},
                {<<"uid">>, <<Random/binary, "@", (node_name_to_binary())/binary>>}
            ],
            {ok, Token} = jwt:encode(<<"HS256">>, Claims, Key),
            Token %% Tokens never expire
    end.

init(Req, State) ->
    Method = cowboy_req:method(Req),
    maybe_process(Req, State, Method).

terminate(_Reason, _Req, _State) ->
    ok.

maybe_process(Req, State, <<"POST">>) ->
    {ok, cowboy_req:reply(200, #{
        <<"content-type">> => <<"text/plain; charset=utf-8">>,
        <<"access-control-allow-origin">> => <<"*">>
    } , generate_token(), Req), State};
maybe_process(Req, State, <<"OPTIONS">>) ->
    {ok, cowboy_req:reply(200, #{
        <<"content-type">> => <<"text/plain; charset=utf-8">>,
        <<"access-control-allow-origin">> => <<"*">>,
        <<"access-control-allow-headers">> => <<"authorization">>,
        <<"access-control-allow-method">> => <<"POST">>
    } , <<>>, Req), State};
maybe_process(Req, _State, _) ->
    cowboy_req:reply(405, Req).

node_name_to_binary() ->
    re:replace(atom_to_list(node()),"@","",[{return, binary}]).

-ifdef(TEST).
auth_test() ->
    Iss = <<"test inc.">>,
    Key = <<"53F61451CAD6231FDCF6859C6D5B88C1EBD5DC38B9F7EBD990FADD4EB8EB9063">>,
    Uid = <<"tester@test.com">>,

    application:start(crypto),
    application:set_env(spread, jwt_key, Key),
    application:set_env(spread, jwt_iss, Iss),

    Claims = [
        {uid, Uid}
    ],
    ExpirationSeconds = 86400,

    {ok, Token} = jwt:encode(<<"HS256">>, Claims, ExpirationSeconds, Key),
    ?assertEqual(auth(Token), {error, bad_issuer}),

    ?assertEqual(auth(Key), {error, invalid_token}),

    {ok, Token2} = jwt:encode(<<"HS256">>, [{iss, Iss} | Claims], ExpirationSeconds, Key),
    ?assertEqual(auth(Token2), Uid),

    {ok, Token3} = jwt:encode(<<"HS256">>, [{iss, Iss}], ExpirationSeconds, Key),
    ?assertEqual(auth(Token3), {error, no_uid}).

-endif.