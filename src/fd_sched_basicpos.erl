-module(fd_sched_basicpos).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, hint/2, to_req_list/1]).

-record(rw_state,
        { reqs            :: array:arary()
        , rng             :: rand:state()
        , dequeue_counter :: integer()
        , guidance        :: [term()]
        , seed            :: term()
        , trace_tab       :: ets:tid()
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
    #rw_state{ reqs = array:new()
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
             , trace_tab = proplists:get_value(trace_tab, Opts, undefined)
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

enqueue_req(#fd_delay_req{data = Data} = ReqInfo, #rw_state{reqs = Reqs, rng = Rng} =  State) ->
    {P, NewRng} = new_priority(Data, Rng),
    {ok, State#rw_state{reqs = array:set(array:size(Reqs), {ReqInfo, P}, Reqs), rng = NewRng}}.

dequeue_req(#rw_state{dequeue_counter = Cnt, guidance = [Cnt]} = State) ->
    Seed = rand:export_seed_s(rand:seed_s(exrop)),
    io:format(user, "[FD] randomize guidance ~w~n", [Cnt]),
    dequeue_req(maybe_trace(State#rw_state{guidance = [{Cnt, Seed}], seed = Seed}, {randomize_guidance, Cnt}));
dequeue_req(#rw_state{reqs = Reqs, dequeue_counter = Cnt, guidance = [{Cnt, SeedTerm} | G]} = State) ->
    {NewReqs, NewRng} =
        array:foldl(
          fun (Index, {#fd_delay_req{data = Data} = RI, _}, {CurReqs, CurRng}) ->
                  {NewP, NewRng} = new_priority(Data, CurRng),
                  {array:set(Index, {RI, NewP}, CurReqs), NewRng}
          end, {Reqs, rand:seed_s(SeedTerm)}, Reqs),
    io:format(user, "[FD] take guidance ~w, remaining ~w~n", [{Cnt, SeedTerm}, G]),
    dequeue_req(maybe_trace(State#rw_state{reqs = NewReqs, rng = NewRng, dequeue_counter = 0, guidance = G}, {take_guidance, {Cnt, SeedTerm}, G}));
dequeue_req(#rw_state{reqs = Reqs, dequeue_counter = Cnt} = State) ->
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
    NewReqs = array:resize(S - 1, array:set(I, array:get(S - 1, Reqs), Reqs)),
    {ok, Req, State#rw_state{reqs = NewReqs, dequeue_counter = Cnt + 1}}.

hint({get_seed_info, Ref, From}, #rw_state{dequeue_counter = Cnt, seed = Seed} = State) ->
    Reply = {Seed, Cnt},
    io:format(user, "[FD] hint get_seed_info -> ~w~n", [Reply]),
    From ! {Ref, Reply},
    State;
hint({set_guidance, Guidance}, State) ->
    io:format(user, "[FD] hint set_guidance ~w~n", [Guidance]),
    maybe_trace(State#rw_state{dequeue_counter = 0, guidance = Guidance}, {set_guidance, Guidance});
hint(_, State) ->
    State.

maybe_trace(#rw_state{trace_tab = undefined} = S, _) -> S;
maybe_trace(#rw_state{trace_tab = Tab} = S, E) ->
    TC = ets:update_counter(Tab, trace_counter, 1),
    ets:insert(Tab, {TC, firedrill, E})
    S.

to_req_list(#rw_state{reqs = Reqs}) ->
    array:to_list(Reqs).
