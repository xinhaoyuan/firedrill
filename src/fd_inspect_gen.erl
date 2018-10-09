-module(fd_inspect_gen).

-export([parse_transform/2]).

parse_transform(Forms, _Options) ->
    parse_trans:plain_transform(fun do_transform/1, Forms).

do_transform({call, L0, {remote, _, {atom, L2, Mod}, {atom, L3, Func}}, Args})
  when Mod =:= gen_server;
       Mod =:= gen_server2;
       Mod =:= gen_statem;
       Mod =:= gen_fsm,
       Func =:= call;
       Func =:= cast;
       Func =:= sync_send_event;
       Func =:= send_event
       ->
    {cons, _, Node, _} = ArgsList = lists:foldr(
                 fun (A, L) ->
                         Line = element(2, A),
                         {cons, Line, A, L}
                 end,
                 %% using L0 has problem?
                 {nil, L0},
                 parse_trans:plain_transform(fun do_transform/1, Args)),
    {call, L0, {remote, L0, {atom, L0, firedrill}, {atom, L0, apply}},
     [{tuple, L0, [{atom, L0, node}, Node]}, {atom, L2, Mod}, {atom, L3, Func}, ArgsList]};

do_transform(_) ->
    continue.
