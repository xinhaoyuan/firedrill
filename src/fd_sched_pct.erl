-module(fd_sched_pct).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, handle_call/3, handle_cast/2, to_req_list/1]).

-record(state,
        { pri             :: dict:dict(term(), {float(), integer()}) %% from -> priority, last enqueue counter
        , reqs            :: array:arary()
        , reset_depth     :: integer()
        , reset_length    :: integer()
        , reset_remain    :: integer()
        , reset_path      :: [integer()]
        , conc_length     :: integer()
        , rng             :: rand:state()
        , last_reset_enqueue_counter :: integer()
        , enqueue_counter :: integer()
        , dequeue_counter :: integer()
        , guidance        :: [term()]
        , seed            :: term()
        , trace_tab       :: ets:tid()
        }).

build_reset_info(SortedPriList, Base, Depth, Length, Rng) ->
    {Rng1, Path} =
        lists:foldl(
          fun(_, {CurRng, CurPath}) ->
                  {Pos, CurRng1} = rand:uniform_s(Length, CurRng),
                  {CurRng1, [Base + Pos - 1 | CurPath]}
          end, {Rng, []}, lists:seq(1, Depth)),
    {NewPri, Rng2} =
        lists:foldl(
          fun ({F, {_, ECnt}}, {CurPri, CurRng}) ->
                  {P, CurRng1} = rand:uniform_s(CurRng),
                  {dict:store(F, {P + 1, ECnt}, CurPri), CurRng1}
          end, {dict:new(), Rng1}, SortedPriList),
    {Path, NewPri, Rng2}.

try_hit_path(Cnt, Path) ->
    try_hit_path(Cnt, Path, [], false).
try_hit_path(_, [], _, false) ->
    false;
try_hit_path(_, [], R, true) ->
    R;
try_hit_path(Cnt, [Cnt | T], R, false) ->
    try_hit_path(Cnt, T, R, true);
try_hit_path(Cnt, [H | T], R, Matched) ->
    try_hit_path(Cnt, T, [H | R], Matched).

init(Opts) ->
    {Seed, IsExternal} =
        case proplists:get_value(seed, Opts, undefined) of
            undefined ->
                R = rand:export_seed_s(rand:seed_s(exrop)),
                io:format(user, "seed = ~p~n", [R]),
                {R, proplists:is_defined(guidance, Opts)};
            V ->
                {V, true}
        end,
    ResetDepth =
        case os:getenv("FD_PCT_DEPTH") of
            false ->
                case proplists:get_value(reset_depth, Opts) of
                    undefined ->
                        error(need_pct_depth);
                    _Depth ->
                        _Depth
                end;
            _EnvDepth ->
                list_to_integer(_EnvDepth)
        end,
    ResetLength =
        case os:getenv("FD_PCT_LENGTH") of
            false ->
                case proplists:get_value(reset_length, Opts) of
                    undefined ->
                        error(need_pct_length);
                    _Length ->
                        _Length
                end;
            _EnvLength ->
                list_to_integer(_EnvLength)
        end,
    #state{ pri = dict:new()
          , reqs = array:new()
          , reset_depth = ResetDepth
          , reset_length = ResetLength
          , reset_remain = 0
          , reset_path = []
          , conc_length = 0
          , rng = rand:seed_s(Seed)
          , last_reset_enqueue_counter = 0
          , enqueue_counter = 0
          , dequeue_counter = 0
          , seed =
                case IsExternal of
                    true ->
                        undefined;
                    false ->
                        Seed
                end
          , guidance =
                case IsExternal of
                    true ->
                        proplists:get_value(guidance, Opts, []);
                    false ->
                        []
                end
          , trace_tab = proplists:get_value(trace_tab, Opts, undefined)
          }.

enqueue_req(#fd_delay_req{data = Data} = ReqInfo, #state{pri = Pri, reqs = Reqs, enqueue_counter = ECnt, dequeue_counter = Cnt, rng = Rng} =  State) ->
    From = maps:get(from, Data),
    {Rng0, Pri1} =
        case dict:find(From, Pri) of
            {ok, {P, _}} ->
                {Rng, dict:store(From, {P, ECnt}, Pri)};
            error ->
                {P, _NewRng} = rand:uniform_s(Rng),
                {_NewRng, dict:store(From, {P + 1, ECnt}, Pri)}
        end,
    {ReqP, Rng1} = rand:uniform_s(Rng0),
    {ok, State#state{pri = Pri1, reqs = array:set(array:size(Reqs), {ReqInfo, Cnt, Cnt, ReqP}, Reqs), enqueue_counter = ECnt + 1, rng = Rng1}}.

dequeue_req(#state{dequeue_counter = Cnt, guidance = [Cnt]} = State) ->
    Seed = rand:export_seed_s(rand:seed_s(exrop)),
    io:format(user, "[FD] randomize guidance ~w~n", [Cnt]),
    dequeue_req(maybe_trace(State#state{guidance = [{Cnt, Seed}], seed = Seed}, {randomize_guidance, Cnt}));
dequeue_req(#state{dequeue_counter = Cnt, guidance = [{Cnt, SeedTerm} | G]} = State) ->
    io:format(user, "[FD] take guidance ~w, remaining ~w~n", [{Cnt, SeedTerm}, G]),
    dequeue_req(maybe_trace(State#state{reset_remain = 0, reset_path = [], rng = rand:seed_s(SeedTerm), dequeue_counter = 0, guidance = G}, {take_guidance, {Cnt, SeedTerm}, G}));
dequeue_req(#state{pri = Pri, reqs = Reqs, reset_depth = Depth, reset_length = Length, reset_remain = 0,
                   last_reset_enqueue_counter = _LRECnt, enqueue_counter = ECnt,
                   dequeue_counter = Cnt, rng = Rng} = State) ->
    {NewReqs, NewRng0} =
        array:foldl(
          fun (Index, {RI, Birth, _, _}, {CurReqs, CurRng}) ->
                  {P, CurRng1} = rand:uniform_s(CurRng),
                  {array:set(Index, {RI, Birth, Cnt, P}, CurReqs), CurRng1}
          end, {Reqs, Rng}, Reqs),
    SortedPriList =
        lists:sort(
          fun ({_, {_, E1}}, {_, {_, E2}}) ->
                  E1 < E2
          end, dict:to_list(Pri)),
    {Path, Pri1, NewRng1} = build_reset_info(SortedPriList, Cnt, Depth, Length, NewRng0),
    dequeue_req(maybe_trace(State#state{pri = Pri1, reqs = NewReqs, reset_remain = Length, reset_path = Path, last_reset_enqueue_counter = ECnt, rng = NewRng1},
                            {reset_path, Path, Pri1}));
dequeue_req(#state{pri = Pri, reqs = Reqs, %% reset_remain = ResetRemain,
                   reset_path = ResetPath, conc_length = CLen, dequeue_counter = Cnt, rng = Rng} = State) ->
    case array:size(Reqs) of
        1 ->
            {#fd_delay_req{data = ReqData} = Req, Birth, _, _} = array:get(0, Reqs),
            RetData = #{age => Cnt - Birth, weight => maps:get(weight, ReqData, undefined)},
            { ok
            , Req
            , RetData
            , State#state{reqs = array:new(), dequeue_counter = Cnt + 1}};
        _ ->
            {CI, CFrom, _, _} =
                array:foldl(
                  fun (I, {#fd_delay_req{data = #{from := From}}, _, _, ReqP}, none) ->
                          {P, _} = dict:fetch(From, Pri),
                          {I, From, P, ReqP};
                      (I, {#fd_delay_req{data = #{from := From}}, _, _, ReqP}, {_, BestFrom, BestP, BestReqP} = B) ->
                          {P, _} = dict:fetch(From, Pri),
                          if
                              From =:= BestFrom, ReqP > BestReqP ->
                                  {I, BestFrom, BestP, ReqP};
                              From =/= BestFrom, P > BestP ->
                                  {I, From, P, ReqP};
                              true ->
                                  B
                          end
                  end, none, Reqs),
            case try_hit_path(CLen, ResetPath) of
                false ->
                    {#fd_delay_req{data = ReqData} = Req, Birth, _, _} = array:get(CI, Reqs),
                    NewReqList =
                        array:foldr(
                          fun (I, _, L) when I =:= CI ->
                                  L;
                              (_, Item, L) ->
                                  [Item | L]
                          end, [], Reqs),
                    NewReqs = array:from_list(NewReqList),
                    RetData = #{age => Cnt - Birth, weight => maps:get(weight, ReqData, undefined)},
                    { ok
                    , Req
                    , RetData
                    , State#state{reqs = NewReqs,
                                  %% do not reset for now
                                  % reset_remain = ResetRemain - 1,
                                  conc_length = CLen + 1, dequeue_counter = Cnt + 1}};
                NewPath ->
                    {NewFromP, NewRng} = rand:uniform_s(Rng),
                    NewPri =
                        case dict:fetch(CFrom, Pri) of
                            {_, ECnt} ->
                                dict:store(CFrom, {NewFromP, ECnt}, Pri)
                        end,
                    dequeue_req(State#state{pri = NewPri, reset_path = NewPath, rng = NewRng})
            end
    end.

%% maybe_update_conc_length(#state{reqs = Reqs, conc_length = CL, max_conc_length = MaxCL} = State) ->
%%     case array:size(Reqs) of
%%         0 ->
%%             case CL > MaxCL of
%%                 true ->
%%                     State#state{conc_length = 0, reset_remain = 0, max_conc_length = CL};
%%                 false ->
%%                     State#state{conc_length = 0, reset_remain = 0}
%%             end;
%%         _ ->
%%             State#state{conc_length = CL + 1}
%%     end.

handle_call({get_trace_info}, _From, #state{conc_length = CL, dequeue_counter = Cnt, seed = Seed} = State) ->
    Reply = #{seed => Seed, dequeue_count => Cnt, conc_length => CL},
    {reply, Reply, State};
handle_call({set_guidance, Guidance}, _From, State) ->
    {reply, ok, maybe_trace(State#state{dequeue_counter = 0, guidance = Guidance}, {set_guidance, Guidance})};
handle_call(_, _, State) ->
    {reply, ignored, State}.

handle_cast(_, State) ->
    State.

maybe_trace(#state{trace_tab = undefined} = S, _E) ->
    S;
maybe_trace(#state{trace_tab = Tab} = S, E) ->
    TC = ets:update_counter(Tab, trace_counter, 1),
    ets:insert(Tab, {TC, firedrill, E}),
    S.

to_req_list(#state{reqs = Reqs}) ->
    array:to_list(Reqs).
