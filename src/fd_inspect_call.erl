-module(fd_inspect_call).

-export([parse_transform/2]).

parse_transform(Forms, Options) ->
    OptInspectList =
        case proplists:get_value(fd_inspect_call_list, Options) of
            L when is_list(L) -> L;
            _ -> []
        end,
    FileInspectList =
        lists:flatmap(fun ({attribute, _L, fd_inspect_call, V}) -> [V];
                          (_F) -> [] end,
                      Forms),
    InspectList = OptInspectList ++ FileInspectList,
    parse_trans:plain_transform(
      fun (F) ->
              do_transform(F, InspectList)
      end, Forms).

do_transform({call, L0, {remote, _, {atom, L2, Mod}, {atom, L3, Func}}, Args} = Form, List) ->
    Arity = length(Args),
    Key = {Mod, Func, Arity},
    case proplists:get_value(Key, List) of
        undefined -> Form;
        Pos ->
            ArgsList = lists:foldr(
                         fun (A, L) ->
                                 Line = element(2, A),
                                 {cons, Line, A, L}
                         end,
                         %% using L0 has problem?
                         {nil, L0},
                         parse_trans:plain_transform(fun (F) -> do_transform(F, List) end, Args)),
            {call, L0, {remote, L0, {atom, L0, firedrill}, {atom, L0, apply}},
             [{tuple, L0, [{atom, L0, raw_fn_arg}, {atom, L0, Pos}]}, {atom, L2, Mod}, {atom, L3, Func}, ArgsList]}
    end;

do_transform(_, _) ->
    continue.
