-module(spread_jwt_auth).

-export([auth/1]).

%% Sample: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE0ODQ3MzA4OTcsImlhdCI6MTQ4NDczMDI5NywiaXNzIjoiTWFzdCBJbmMuIiwidWlkIjoiMTY4ODgxNyIsImp0aSI6ImZmMGRlYTI5ZWViYjY2MTYwZjliM2YzZTZjZDg3ZDI4IiwiYXVkIjpbImFkbWluaXN0cmF0b3IiLCJieW9jX3VzZXIiLCJlbmRfdXNlciIsImJpbGxpbmciXX0.rbF6Ewv4_vIG1gfDryd2YoE7Srd-mbXS5aI4CkAE-ZA

auth(Authorization) ->
    {ok, Key} = application:get_env(spread, jwt_key),
    {ok, Iss} = application:get_env(spread, jwt_iss),
    case jwt:decode(Authorization, Key) of
        {ok, Claims} ->    
            lager:info("Claims are ~p", [Claims]),
            case maps:get(<<"iss">>, Claims) of
                Iss ->
                    maps:get(<<"uid">>, Claims);
                _ ->
                    error
            end;
        _Any ->
            lager:error("Token said ~p", [_Any]),
            error
    end.
