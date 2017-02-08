-module(spread_jwt_auth).

-export([auth/1]).

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