-module(fd_sched_basicpos).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, handle_call/3, handle_cast/2, to_req_list/1]).

-record(state,
        { reqs            :: array:arary()
        , rng             :: rand:state()
        , dequeue_counter :: integer()
        , guidance        :: [term()]
        , reset_watermark :: float()
        , seed            :: term()
        , trace_tab       :: ets:tid()
        , max_age         :: integer()
        }).

init(Opts) ->
    {Seed, IsExternal} =
        case proplists:get_value(seed, Opts, undefined) of
            undefined ->
                R = rand:export_seed_s(rand:seed_s(exrop)),
                io:format(user, "seed = ~p~n", [R]),
                {R, false};
            V ->
                {V, true}
        end,
    #state{ reqs = array:new()
          , rng = rand:seed_s(Seed)
          , dequeue_counter = 0
          , seed =
                case IsExternal of
                    true ->
                        undefined;
                    false ->
                        Seed
                end
          , reset_watermark =
                proplists:get_value(reset_watermark, Opts, 0.0)
          , guidance =
                case IsExternal of
                    true ->
                        proplists:get_value(guidance, Opts, []);
                    false ->
                        []
                end
          , trace_tab = proplists:get_value(trace_tab, Opts, undefined)
          , max_age = 1000 %% XXX make it configurable
          }.

enqueue_req(#fd_delay_req{data = Data} = ReqInfo, #state{reqs = Reqs, dequeue_counter = Cnt, rng = Rng} =  State) ->
    {P, NewRng} = fd_rand_helper:new_priority(Data, Rng),
    {ok, State#state{reqs = array:set(array:size(Reqs), {ReqInfo, Cnt, P}, Reqs), rng = NewRng}}.

dequeue_req(#state{dequeue_counter = Cnt, guidance = [Cnt]} = State) ->
    Seed = rand:export_seed_s(rand:seed_s(exrop)),
    io:format(user, "[FD] randomize guidance ~w~n", [Cnt]),
    dequeue_req(maybe_trace(State#state{guidance = [{Cnt, Seed}], seed = Seed}, {randomize_guidance, Cnt}));
dequeue_req(#state{reqs = Reqs, dequeue_counter = Cnt, guidance = [{Cnt, SeedTerm} | G]} = State) ->
    {NewReqs, NewRng} =
        array:foldl(
          fun (Index, {#fd_delay_req{data = Data} = RI, _, _}, {CurReqs, CurRng}) ->
                  {NewP, NewRng} = fd_rand_helper:new_priority(Data, CurRng),
                  {array:set(Index, {RI, Cnt, NewP}, CurReqs), NewRng}
          end, {Reqs, rand:seed_s(SeedTerm)}, Reqs),
    io:format(user, "[FD] take guidance ~w, remaining ~w~n", [{Cnt, SeedTerm}, G]),
    dequeue_req(maybe_trace(State#state{reqs = NewReqs, rng = NewRng, dequeue_counter = 0, guidance = G}, {take_guidance, {Cnt, SeedTerm}, G}));
dequeue_req(#state{reqs = Reqs, dequeue_counter = Cnt, reset_watermark = ResetWM, rng = Rng} = State) ->
    {CI, CP} = array:foldl(
               fun (I, {_, _, P}, none) ->
                       {I, P};
                   (I, {_, _, P}, {BestI, BestP}) ->
                       if
                           P > BestP ->
                               {I, P};
                           true ->
                               {BestI, BestP}
                       end
               end, none, Reqs),
    {Req, _, _} = array:get(CI, Reqs),
    {NewReqs, NewRng} =
        case CP < ResetWM of
            true ->
                {NewReqList, NewRng0} =
                    array:foldr(
                      fun (I, _, Acc) when I =:= CI ->
                              Acc;
                          (_, {Data, _, _}, {L, CurRng}) ->
                              {NewP, NewRng} = fd_rand_helper:new_priority(Data, CurRng),
                              {[{Data, Cnt, NewP} | L], NewRng}
                      end, {[], Rng}, Reqs),
                {array:from_list(NewReqList), NewRng0};
            false ->
                {NewReqList, NewRng0} =
                    array:foldr(
                      fun (I, _, Acc) when I =:= CI ->
                              Acc;
                          (_, {Data, Birth, _}, {L, CurRng})
                            when Cnt - Birth > State#state.max_age ->
                              {NewP, NewRng} = fd_rand_helper:new_priority(Data, CurRng),
                              {[{Data, Cnt, NewP} | L], NewRng};
                          (_, Item, {L, CurRng}) ->
                              {[Item | L], CurRng}
                      end, {[], Rng}, Reqs),
                {array:from_list(NewReqList), NewRng0}
        end,
    {ok, Req, State#state{reqs = NewReqs, dequeue_counter = Cnt + 1, rng = NewRng}}.

handle_call({get_trace_info}, _From, #state{dequeue_counter = Cnt, seed = Seed} = State) ->
    Reply = #{seed => Seed, dequeue_count => Cnt},
    {reply, Reply, State};
handle_call({set_guidance, Guidance}, _From, State) ->
    {reply, ok, maybe_trace(State#state{dequeue_counter = 0, guidance = Guidance}, {set_guidance, Guidance})};
handle_call(_, _, State) ->
    {reply, ignored, State}.

handle_cast(_, State) ->
    State.

maybe_trace(#state{trace_tab = undefined} = S, _) -> S;
maybe_trace(#state{trace_tab = Tab} = S, E) ->
    TC = ets:update_counter(Tab, trace_counter, 1),
    ets:insert(Tab, {TC, firedrill, E}),
    S.

to_req_list(#state{reqs = Reqs}) ->
    array:to_list(Reqs).
