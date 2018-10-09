-module(bang_pt_test).

-include_lib("eunit/include/eunit.hrl"). 

-compile({parse_transform, fd_inspect_bang}).

transform_test_() ->
    [?_test(test_1())].

test_1() ->
    firedrill:start([{scheduler, {pos, []}}, verbose_dequeue]),
    Me = self(),
    spawn(fun () -> Me ! hello end),
    receive hello ->
            ok end,
    firedrill:stop(),
    firedrill:start([{scheduler, {rw, []}}, verbose_dequeue]),
    Me = self(),
    spawn(fun () -> Me ! hello end),
    receive hello ->
            ok end,
    firedrill:stop().
