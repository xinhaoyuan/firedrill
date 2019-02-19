-module(fd_sched_rapos).

%% This is the algorithm from paper:
%% Effective random testing of concurrent programs. In proceeding ASE '07. Koushik Sen.

%% The original RAPOS schedules a program in batch, which is tricky to
%% implement in our framework. Instead, our implementation generatively
%% schedules the `batch` by picking one at a time.

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, handle_call/3, handle_cast/2, to_req_list/1]).

-record(state, { rng :: rand:state()
               , may_skip :: boolean()
               , cand :: array:array()
               , next :: array:array()
               , skipped :: array:array()
               }).

init(Opts) ->
    Seed =
        case proplists:get_value(seed, Opts, undefined) of
            undefined ->
                R = rand:export_seed_s(rand:seed_s(exrop)),
                io:format(user, "seed = ~p~n", [R]),
                R;
            V -> V
        end,
    #state{
       rng = rand:seed_s(Seed),
       may_skip = false,
       cand = array:new(),
       next = array:new(),
       skipped = array:new()
      }.

enqueue_req(ReqInfo, #state{next = Next} =  State) ->
    %% new opetations are always considered as "affected" (enabled by previous requests),
    %% so put them into `next`
    {ok, State#state{next = array:set(array:size(Next), ReqInfo, Next)}}.

dequeue_req(#state{may_skip = MaySkip, cand = Cand, rng = Rng, next = Next, skipped = Skipped} = State) ->
    case array:size(Cand) of
        0 ->
            %% At this moment, `next` is `schedulable` in paper
            case array:size(Next) of
                0 ->
                    %% according to the paper, select a random enabled request in skipped, which must in `skippped`.
                    {R, NewRng} = rand:uniform_s(array:size(Skipped), Rng), I = R - 1,
                    Req = array:get(I, Skipped),
                    NewSkipped = array:from_list(
                                   array:foldr(fun (Idx, _, Acc) when Idx =:= I ->
                                                       Acc;
                                                (_, ReqB, SkippedList) ->
                                                       [ReqB | SkippedList]
                                               end, [], Skipped)),
                    dequeue_req(State#state{may_skip = false, cand = array:from_list([Req]), rng = NewRng, skipped = NewSkipped});
                _ ->
                    dequeue_req(State#state{may_skip = false, cand = Next, next = array:new()})
            end;
        S ->
            {R, NewRng0} = rand:uniform_s(S, Rng), I = R - 1,
            {P, NewRng} =
                case MaySkip of
                    true -> rand:uniform_s(NewRng0);
                    false -> {0, NewRng0}
                end,
            Req = array:get(I, Cand),
            case P < 0.5 of
                true ->
                    %% Move all racing requests in `cand` and `skipped` to next
                    {NewCandList, NewNextList0} =
                        array:foldr(fun (Idx, _, Acc) when Idx =:= I ->
                                            Acc;
                                        (_, ReqB, {CandList, NextList}) ->
                                            case is_racing(Req, ReqB) of
                                                true ->
                                                    {CandList, [ReqB | NextList]};
                                                false ->
                                                    {[ReqB | CandList], NextList}
                                            end
                                    end, {[], array:to_list(Next)}, Cand),
                    {NewSkippedList, NewNextList} =
                        array:foldr(fun (_, ReqB, {SkippedList, NextList}) ->
                                            case is_racing(Req, ReqB) of
                                                true ->
                                                    {SkippedList, [ReqB | NextList]};
                                                false ->
                                                    {[ReqB | SkippedList], NextList}
                                            end
                                    end, {[], NewNextList0}, Skipped),
                    NewCand = array:from_list(NewCandList),
                    NewNext = array:from_list(NewNextList),
                    NewSkipped = array:from_list(NewSkippedList),
                    {ok, Req, undefined, State#state{may_skip = true, rng = NewRng, cand = NewCand, next = NewNext, skipped = NewSkipped}};
                false ->
                    %% Skip the current request to `skipped`
                    NewCand = array:from_list(
                                array:foldr(fun (Idx, _, Acc) when Idx =:= I ->
                                                    Acc;
                                                (_, ReqB, CandList) ->
                                                    [ReqB | CandList]
                                            end, [], Cand)),
                    NewSkipped = array:set(array:size(Skipped), Req, Skipped),
                    dequeue_req(State#state{may_skip = true, cand = NewCand, rng = NewRng, skipped = NewSkipped})
            end
    end.

handle_call(_, _, State) ->
    {reply, ignored, State}.

handle_cast(_, State) ->
    State.

to_req_list(#state{cand = Cand, next = Next, skipped = Skipped}) ->
    array:to_list(Cand) ++ array:to_list(Next) ++ array:to_list(Skipped).

is_racing(#fd_delay_req{to = global}, #fd_delay_req{}) ->
    false;
is_racing(#fd_delay_req{}, #fd_delay_req{to = global}) ->
    false;
is_racing(#fd_delay_req{to = To}, #fd_delay_req{to = To}) ->
    true;
is_racing(_, _) ->
    false.
