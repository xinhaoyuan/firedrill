-module(transform_test).

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, fd_inspect_bang}).
-compile({parse_transform, fd_inspect_call}).

%% fd_inspect_call will read these attributes and replace all remote calls to these functions
-fd_inspect_call({{proplists, get_value, 2}, 0}).
%% the value associate with the function spec is the position of the arguments corresponding to the communication object. This is for POS.
-fd_inspect_call({{lists, nth, 2}, 2}).

bang_test() ->
    firedrill:start([{scheduler, {rw, []}}, verbose_dequeue, verbose_final]),
    Me = self(),
    spawn(fun () -> Me ! hello end),
    receive hello ->
            ok end,
    firedrill:stop().

call_test() ->
    firedrill:start([{scheduler, {rw, []}}, verbose_dequeue, verbose_final]),
    proplists:get_value(a, [a]),
    lists:nth(1, [a]),
    firedrill:stop(),
    bang_test().
