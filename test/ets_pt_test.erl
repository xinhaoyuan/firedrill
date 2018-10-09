-module(ets_pt_test).

-include_lib("eunit/include/eunit.hrl"). 

-compile({parse_transform, fd_inspect_ets}).

transform_test_() ->
    [?_test(test_1())].

test_1() ->
    firedrill:start([verbose_dequeue]),
    ets:new(abc, [named_table]),
    ets:insert(abc, {a,b}),
    firedrill:stop().
