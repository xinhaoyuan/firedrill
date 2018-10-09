-module(fd_inspect_ets).

-export([parse_transform/2]).

parse_transform(Forms, _Options) ->
    parse_trans:plain_transform(fun do_transform/1, Forms).

do_transform({call, L0, {remote, _, {atom, L2, Mod}, {atom, L3, EtsFuncName}}, Args})
  when Mod =:= ets;
       Mod =:= mnesia,
       EtsFuncName =:= insert;
       EtsFuncName =:= insert_new;
       EtsFuncName =:= delete;
       EtsFuncName =:= delete_object;
       EtsFuncName =:= update_counter;
       EtsFuncName =:= lookup;
       EtsFuncName =:= lookup_element;
       EtsFuncName =:= match andalso length(Args) =:= 2;
       EtsFuncName =:= match_delete;
       EtsFuncName =:= select andalso length(Args) =:= 2;
       EtsFuncName =:= select_count;
       EtsFuncName =:= select_selete;
       EtsFuncName =:= select_reverse andalso length(Args) =:= 2;
       EtsFuncName =:= member;
       EtsFuncName =:= safe_fixtable;
       EtsFuncName =:= take;
       EtsFuncName =:= update_element;
       EtsFuncName =:= update_counter
       ->
    {cons, _, Tab, _} = ArgsList = lists:foldr(
                 fun (A, L) ->
                         Line = element(2, A),
                         {cons, Line, A, L}
                 end,
                 %% using L0 has problem?
                 {nil, L0},
                 parse_trans:plain_transform(fun do_transform/1, Args)),
    {call, L0, {remote, L0, {atom, L0, firedrill}, {atom, L0, apply}},
     %% the first 'ets' is only a label in firedrill
     [{tuple, L0, [{atom, L0, ets}, Tab]}, {atom, L2, Mod}, {atom, L3, EtsFuncName}, ArgsList]};

do_transform(_) ->
    continue.
