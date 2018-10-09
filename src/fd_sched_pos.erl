-module(fd_sched_pos).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, hint/2, to_req_list/1]).

-record(rw_state, {reqs :: array:arary()
                  ,rng :: rand:state()}).

init(Opts) ->
    Seed =
        case proplists:get_value(seed, Opts, undefined) of
            undefined ->
                R = rand:export_seed_s(rand:seed_s(exrop)),
                io:format(user, "seed = ~p~n", [R]),
                R;
            V -> V
        end,
    #rw_state{
       reqs = array:new(),
       rng = rand:seed_s(Seed)
      }.

enqueue_req(ReqInfo, #rw_state{reqs = Reqs, rng = Rng} =  State) ->
    {P, NewRng} = rand:uniform_s(Rng),
    {ok, State#rw_state{reqs = array:set(array:size(Reqs), {ReqInfo, P}, Reqs), rng = NewRng}}.

dequeue_req(#rw_state{reqs = Reqs, rng = Rng} = State) ->
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
    {NewRng, NewReqList} =
        lists:foldl(fun ({ReqB, P}, {TRng, ReqList}) ->
                            if
                                Req#fd_delay_req.to =:= ReqB#fd_delay_req.to ->
                                    %% Reset priority for the conflict
                                    {NewP, NewRng} = rand:uniform_s(TRng),
                                    {NewRng, [{ReqB, NewP}|ReqList]};
                                true ->
                                    {TRng, [{ReqB, P}|ReqList]}
                            end
                    end, {Rng, []}, array:to_list(RArr)),
    NewReqs = array:from_list(NewReqList),
    {ok, Req, State#rw_state{reqs = NewReqs, rng = NewRng}}.

hint(_, State) ->
    State.

to_req_list(#rw_state{reqs = Reqs}) ->
    array:to_list(Reqs).
