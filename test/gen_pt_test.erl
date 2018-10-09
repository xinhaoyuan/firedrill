-module(gen_pt_test).

-include_lib("eunit/include/eunit.hrl").

-compile({parse_transform, fd_inspect_gen}).

transform_test_() ->
    [?_test(test_1())].

test_1() ->
    firedrill:start([verbose_dequeue]),
    gen_server:cast(noname, []),
    firedrill:stop().
