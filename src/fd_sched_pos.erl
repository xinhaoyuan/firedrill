-module(fd_sched_pos).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, handle_call/3, handle_cast/2, to_req_list/1]).

-record(state,
        { reqs                 :: array:arary()
        , rng                  :: rand:state()
        , dequeue_counter      :: integer()
        , guidance             :: [term()]
        , variant              :: atom()
        , seed                 :: term()
        , priority_reset_count :: integer()
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
    #state{
       reqs = array:new(),
       rng = rand:seed_s(Seed),
       dequeue_counter = 0,
       seed =
           case IsExternal of
               true ->
                   undefined;
               false ->
                   Seed
           end,
       guidance =
           case IsExternal of
               true ->
                   proplists:get_value(guidance, Opts, []);
               false ->
                   []
           end,
       variant = proplists:get_value(variant, Opts, undefined),
       priority_reset_count = 0
      }.

new_priority(#{delay_level := Level}, Rng) ->
    {P, NewRng} = rand:uniform_s(Rng),
    {P - Level, NewRng};
new_priority(#{weight := Weight}, Rng) ->
    {UP, NewRng} = rand:uniform_s(Rng),
    {math:pow(UP, Weight), NewRng};
new_priority(_, Rng) ->
    {UP, NewRng} = rand:uniform_s(Rng),
    {UP, NewRng}.

enqueue_req(#fd_delay_req{data = Data} = ReqInfo, #state{reqs = Reqs, rng = Rng} =  State) ->
    {P, NewRng} = new_priority(Data, Rng),
    {ok, State#state{reqs = array:set(array:size(Reqs), {ReqInfo, P}, Reqs), rng = NewRng}}.

dequeue_req(#state{dequeue_counter = Cnt, guidance = [Cnt]} = State) ->
    Seed = rand:export_seed_s(rand:seed_s(exrop)),
    dequeue_req(State#state{guidance = [{Cnt, Seed}], seed = Seed});
dequeue_req(#state{reqs = Reqs, dequeue_counter = Cnt, guidance = [{Cnt, SeedTerm} | G]} = State) ->
    {NewReqs, NewRng} =
        array:foldl(
          fun (Index, {#fd_delay_req{data = Data} = RI, _}, {CurReqs, CurRng}) ->
                  {NewP, NewRng} = new_priority(Data, CurRng),
                  {array:set(Index, {RI, NewP}, CurReqs), NewRng}
          end, {Reqs, rand:seed_s(SeedTerm)}, Reqs),
    dequeue_req(State#state{reqs = NewReqs, rng = NewRng, dequeue_counter = 0, guidance = G});
dequeue_req(#state{reqs = Reqs, rng = Rng, dequeue_counter = Cnt, priority_reset_count = PRCnt, variant = Variant} = State) ->
    {I, _} = array:foldl(
               fun (I, {_, P}, none) ->
                       {I, P};
                   (I, {_, P}, {BestI, BestP}) ->
                       if
                           P > BestP ->
                               {I, P};
                           true ->
                               {BestI, BestP}
                       end
               end, none, Reqs),
    S = array:size(Reqs),
    {Req, _} = array:get(I, Reqs),
    RArr = array:resize(S - 1, array:set(I, array:get(S - 1, Reqs), Reqs)),
    %% Update priorities
    {NewRng, NewReqList, ResetCount} =
        lists:foldl(fun ({ReqB, P}, {TRng, ReqList, CurResetCount}) ->
                            case is_racing(Req, #fd_delay_req{data = Data} = ReqB) of
                                true ->
                                    case Variant of
                                        undefined ->
                                            %% Reset priority for the conflict
                                            {NewP, NewRng} = new_priority(Data, TRng),
                                            {NewRng, [{ReqB, NewP}|ReqList], CurResetCount + 1};
                                        lazy ->
                                            NewCount = maps:get(lazy_counter, Data, 1) + 1,
                                            NewData = Data#{lazy_counter => NewCount},
                                            {Ran, NewRng0} = rand:uniform_s(TRng),
                                            case Ran * NewCount < 1 of
                                                true ->
                                                    {NewP, NewRng} = new_priority(NewData, NewRng0),
                                                    {NewRng, [{ReqB#fd_delay_req{data = NewData}, NewP}|ReqList], CurResetCount + 1};
                                                false ->
                                                    {NewRng0, [{ReqB#fd_delay_req{data = NewData}, P}|ReqList], CurResetCount}
                                            end
                                    end;
                                false ->
                                    {TRng, [{ReqB, P}|ReqList], CurResetCount}
                            end
                    end, {Rng, [], 0}, array:to_list(RArr)),
    NewReqs = array:from_list(NewReqList),
    {ok, Req, State#state{reqs = NewReqs,
                          rng = NewRng,
                          dequeue_counter = Cnt + 1,
                          priority_reset_count = PRCnt + ResetCount}}.

handle_call({get_trace_info}, _From, #state{dequeue_counter = Cnt, seed = Seed, priority_reset_count = PriorityResetCount} = State) ->
    Reply = #{seed => Seed, dequeue_count => Cnt, priority_reset_count => PriorityResetCount},
    io:format(user, "[FD] get_trace_info => ~p~n", [Reply]),
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
is_racing(_, _) ->
    false.
