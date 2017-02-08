-module(spread_pid).

-export([
    send/2
    ]).

-type spread_pid() :: pid() | {pid(), any()}.
-export_type([spread_pid/0]).

-spec send(any(), spread_pid()) -> ok.
send(Payload, Pid) when is_pid(Pid) ->
    Pid ! Payload.
% send(Payload, {Pid, Any}) when is_pid(Pid) ->
%     Pid ! {Any, Payload}.
