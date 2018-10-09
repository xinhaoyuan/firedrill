-module(rr_test1).

-include("firedrill.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([{parse_transform, fd_inspect_bang}]).

rr_test() ->
    io:format(user, "Now is ~p~n", [erlang:localtime()]),
    Size = 2,
    firedrill:start([{scheduler, {pos, []}}]),
    Me = self(),
    PA = spawn(fun () ->
                       lists:foreach(fun (_) ->
                                             Me ! self()
                                     end, lists:seq(1, Size))
          end),
    PB = spawn(fun () ->
                       lists:foreach(fun (_) ->
                                             Me ! self()
                                     end, lists:seq(1, Size))
               end),
    recv_loop(none, Size * 2, Size * 2),
    firedrill:stop().

recv_loop(_, _, 0) ->
    ?assert(false);
recv_loop(_, 0, _) ->
    ok;
recv_loop(Last, C, I) ->
    receive
        Last ->
            recv_loop(Last, C - 1, I);
        N ->
            recv_loop(N, C - 1, I - 1)
    end.
