-module(fd_sched_pos).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, handle_call/3, handle_cast/2, to_req_list/1]).

-record(state,
        { reqs                 :: array:arary()
        , rng                  :: rand:state()
        , dequeue_counter      :: integer()
        , guidance             :: [term()]
        , variant              :: undefined | basic | lazy | simrw
        , is_racing_fun        :: fun()
        , seed                 :: term()
        , priority_reset_count :: integer()
        , max_age              :: integer()
        }).

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
          , guidance =
                case IsExternal of
                    true ->
                        proplists:get_value(guidance, Opts, []);
                    false ->
                        []
                end
          , variant = proplists:get_value(variant, Opts, undefined)
          , is_racing_fun = proplists:get_value(is_racing_fun, Opts, fun is_racing/2)
          , priority_reset_count = 0
          , max_age = 1000 %% XXX make it configurable
          }.

enqueue_req(#fd_delay_req{data = Data} = Req, #state{reqs = Reqs, rng = Rng, dequeue_counter = Cnt} =  State) ->
    {P, NewRng} = fd_rand_helper:new_priority(Data, Rng),
    {ok, State#state{reqs = array:set(array:size(Reqs), {Req, #{birth => Cnt, last_reset => Cnt, reset => []}, P}, Reqs), rng = NewRng}}.

dequeue_req(#state{dequeue_counter = Cnt, guidance = [Cnt]} = State) ->
    Seed = rand:export_seed_s(rand:seed_s(exrop)),
    dequeue_req(State#state{guidance = [{Cnt, Seed}], seed = Seed});
dequeue_req(#state{reqs = Reqs, dequeue_counter = Cnt, guidance = [{Cnt, SeedTerm} | G]} = State) ->
    {NewReqs, NewRng} =
        array:foldl(
          fun (Index, {#fd_delay_req{data = Data} = Req, ReqInfo, _}, {CurReqs, CurRng}) ->
                  {NewP, NewRng} = fd_rand_helper:new_priority(Data, CurRng),
                  {array:set(Index, {Req, ReqInfo#{last_reset := Cnt, reset := [Cnt - maps:get(last_reset, ReqInfo) | maps:get(reset, ReqInfo)]}, NewP}, CurReqs), NewRng}
          end, {Reqs, rand:seed_s(SeedTerm)}, Reqs),
    dequeue_req(State#state{reqs = NewReqs, rng = NewRng, dequeue_counter = 0, guidance = G});
dequeue_req(#state{reqs = Reqs, rng = Rng, dequeue_counter = Cnt, priority_reset_count = PRCnt, variant = Variant, is_racing_fun = IsRacingFun} = State) ->
    {I, _} = array:foldl(
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
    S = array:size(Reqs),
    {#fd_delay_req{data = ReqData} = Req, ReqInfo, _} = array:get(I, Reqs),
    RArr = array:resize(S - 1, array:set(I, array:get(S - 1, Reqs), Reqs)),
    %% Update priorities
    {NewRng, NewReqList, ResetCount} =
        lists:foldl(fun ({#fd_delay_req{data = Data} = ReqB, #{last_reset := LastReset} = Info, P}, {TRng, ReqList, CurResetCount}) ->
                            {TRng1, Info1, ToReset} =
                                case Variant of
                                    simrw ->
                                        {TRng, Info, true};
                                    basic ->
                                        {TRng, Info, false};
                                    _ ->
                                        case IsRacingFun(Req, ReqB) of
                                            true ->
                                                case Variant of
                                                    undefined ->
                                                        {TRng, Info, true};
                                                    lazy ->
                                                        NewCount = maps:get(lazy_counter, Info, 1) + 1,
                                                        NewInfo = Info#{lazy_counter => NewCount},
                                                        {Ran, NewRng0} = rand:uniform_s(TRng),
                                                        case Ran * NewCount < 1 of
                                                            true ->
                                                                {NewRng0, NewInfo, true};
                                                            false ->
                                                                {NewRng0, NewInfo, false}
                                                        end
                                                end;
                                            false ->
                                                {TRng, Info, false}
                                        end
                                end,
                            case ToReset orelse Cnt - LastReset > State#state.max_age of
                                true ->
                                    {NewP, TRng2} = fd_rand_helper:new_priority(Data, TRng1),
                                    {TRng2, [{ReqB, Info1#{last_reset := Cnt, reset := [Cnt - LastReset | maps:get(reset, Info)]}, NewP}|ReqList], CurResetCount + 1};
                                false ->
                                    {TRng1, [{ReqB, Info1, P}|ReqList], CurResetCount}
                            end
                    end, {Rng, [], 0}, array:to_list(RArr)),
    NewReqs = array:from_list(NewReqList),
    { ok
    , Req
    , #{ age => Cnt - maps:get(birth, ReqInfo)
       , weight => maps:get(weight, ReqData, undefined)
         %% dummy head to avoid string format
       , reset => [head, Cnt - maps:get(last_reset, ReqInfo) | maps:get(reset, ReqInfo)]}
    , State#state{reqs = NewReqs,
                  rng = NewRng,
                  dequeue_counter = Cnt + 1,
                  priority_reset_count = PRCnt + ResetCount}}.

handle_call({get_trace_info}, _From, #state{dequeue_counter = Cnt, seed = Seed, priority_reset_count = PriorityResetCount} = State) ->
    Reply = #{seed => Seed, dequeue_count => Cnt, priority_reset_count => PriorityResetCount},
    {reply, Reply, State};
handle_call({set_guidance, Guidance}, _From, State) ->
    {reply, ok, State#state{dequeue_counter = 0, guidance = Guidance}};
handle_call(_, _, State) ->
    {reply, ignored, State}.

handle_cast(_, State) ->
    State.

to_req_list(#state{reqs = Reqs}) ->
    array:to_list(Reqs).

is_racing(#fd_delay_req{to = global}, #fd_delay_req{}) ->
    false;
is_racing(#fd_delay_req{}, #fd_delay_req{to = global}) ->
    false;
is_racing(#fd_delay_req{to = To}, #fd_delay_req{to = To}) ->
    true;
is_racing(#fd_delay_req{}, #fd_delay_req{}) ->
    false.
