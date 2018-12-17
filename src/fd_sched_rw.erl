-module(fd_sched_rw).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, hint/2, to_req_list/1]).

-record(rw_state, { reqs :: array:arary()
                  , rng :: rand:state()}).

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

enqueue_req(ReqInfo, #rw_state{reqs = Reqs} =  State) ->
    {ok, State#rw_state{reqs = array:set(array:size(Reqs), ReqInfo, Reqs)}}.

dequeue_req(#rw_state{reqs = Reqs, rng = Rng} = State) ->
    S = array:size(Reqs),
    {R, NewRng} = rand:uniform_s(S, Rng), I = R - 1,
    Req = array:get(I, Reqs),
    NewReqs = array:resize(S - 1, array:set(I, array:get(S - 1, Reqs), Reqs)),
    {ok, Req, State#rw_state{reqs = NewReqs, rng = NewRng}}.

%% This simply replies undefined instead of ignoring to make morpheus happy.
hint({get_seed_info, Ref, From}, State) ->
    Reply = undefined,
    io:format(user, "[FD] hint get_seed_info -> ~w~n", [Reply]),
    From ! {Ref, Reply},
    State;
hint(_, State) ->
    State.

to_req_list(#rw_state{reqs = Reqs}) ->
    array:to_list(Reqs).
