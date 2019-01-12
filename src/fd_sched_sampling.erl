-module(fd_sched_sampling).

-include("firedrill.hrl").

%% scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, hint/2, to_req_list/1]).

-record(spl_state, {rng               :: rand:state()
                   ,threshold         :: float()
                   ,to_buffer         :: boolean()
                   ,buffer            :: queue:queue()
                   ,inner_sched       :: module()
                   ,inner_sched_state :: any()
                   }).

init(Opts) ->
    Scheduler = proplists:get_value(inner_scheduler, Opts, {naive, []}),
    {InnerMod, InnerOpts} =
        firedrill:get_module_opts(element(1, Scheduler), element(2, Scheduler)),
    Seed =
        case proplists:get_value(seed, Opts, undefined) of
            undefined ->
                R = rand:export_seed_s(rand:seed_s(exrop)),
                io:format(user, "seed = ~p~n", [R]),
                R;
            V -> V
        end,
    #spl_state{
       rng = rand:seed_s(Seed),
       threshold = proplists:get_value(probability, Opts, 1.0),
       to_buffer = proplists:get_value(buffer, Opts, false),
       buffer = queue:new(),
       inner_sched = InnerMod,
       inner_sched_state = InnerMod:init(InnerOpts)
      }.

enqueue_req(Req, #spl_state{rng = Rng, threshold = Th, to_buffer = ToBuffer, inner_sched = Sched, inner_sched_state = InnerState} =  State) ->
    {P, NewRng} = rand:uniform_s(Rng),
    if
        P >= Th ->
            case ToBuffer of
                true ->
                    {ok, State#spl_state{buffer = queue:in(Req, State#spl_state.buffer)}};
                false ->
                    {rejected, State#spl_state{rng = NewRng}}
            end;
        true ->
            {Ret, NewInnerState} = Sched:enqueue_req(Req, InnerState),
            case Ret =:= rejected andalso ToBuffer of
                true ->
                    {ok, State#spl_state{rng = NewRng, inner_sched_state = NewInnerState,
                                         buffer = queue:in(Req, State#spl_state.buffer)}};
                false ->
                    {Ret, State#spl_state{rng = NewRng, inner_sched_state = NewInnerState}}
            end
    end.

dequeue_req(#spl_state{inner_sched = Sched, buffer = Buffer, inner_sched_state = InnerState} = State) ->
    case queue:is_empty(Buffer) of
        false ->
            {{value, R}, NewBuffer} = queue:out(Buffer),
            {ok, R, State#spl_state{buffer = NewBuffer}};
        true ->
            #spl_state{inner_sched = Sched, inner_sched_state = InnerState} = State,
            case Sched:dequeue_req(InnerState) of
                {ok, Req, NewInnerState} ->
                    {ok, Req, State#spl_state{inner_sched_state = NewInnerState}};
                {none, NewInnerState} ->
                    {none, State#spl_state{inner_sched_state = NewInnerState}}
            end
    end.

hint(H, #spl_state{inner_sched = Sched, inner_sched_state = InnerState} = State) ->
    State#spl_state{inner_sched_state = Sched:hint(H, InnerState)}.

to_req_list(#spl_state{inner_sched = Sched, buffer = Buffer, inner_sched_state = InnerState}) ->
    queue:to_list(Buffer) ++ Sched:to_req_list(InnerState).
