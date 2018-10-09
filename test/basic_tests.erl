-module(basic_tests).
-include("firedrill.hrl").
-include_lib("eunit/include/eunit.hrl").

api_test() ->
    firedrill:start([{scheduler, {pos, []}}, verbose_dequeue, verbose_final]),
    firedrill:send(self(), api_hello),
    receive api_hello -> ok end,
    firedrill:stop(),
    firedrill:start([{scheduler, {basicpos, []}}, verbose_dequeue, verbose_final]),
    firedrill:send(self(), api_hello),
    receive api_hello -> ok end,
    firedrill:stop(),
    firedrill:start([{scheduler, {rw, []}}, verbose_dequeue, verbose_final]),
    firedrill:send(self(), api_hello),
    receive api_hello -> ok end,
    firedrill:stop(),
    firedrill:start([{scheduler, {sampling, []}}, {probability, 0.5}, verbose_dequeue, verbose_final]),
    firedrill:send(self(), api_hello),
    receive api_hello -> ok end,
    firedrill:stop(),
    ok.

ping_test() ->
    firedrill:start([{scheduler, {pos, []}}, verbose_dequeue, verbose_final]),
    firedrill:send(self(), api_hello),
    io:format(user, "Ping result = ~p~n", [firedrill:ping_scheduler()]),
    receive api_hello -> ok end,
    firedrill:stop(),
    ok.
