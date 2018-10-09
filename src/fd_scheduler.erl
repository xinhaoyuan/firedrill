-module(fd_scheduler).

-include("firedrill.hrl").

-export([start_entry/2, start_slave_entry/2, stop/1]).

%% default scheduler callbacks
-export([init/1, enqueue_req/2, dequeue_req/1, hint/2, to_req_list/1]).

-record(fd_opts, { verbose_dequeue         :: boolean()
                 , verbose_final           :: boolean()}).
-record(dist_state, { slave                :: pid()
                    }).
-record(state, { dist_states               :: none | dict:dict()
               , busy_counter              :: non_neg_integer()
               , dequeue_counter           :: non_neg_integer()
               , failure_counter           :: non_neg_integer()
               , current_failure_counter   :: non_neg_integer()
               , kick_counter              :: non_neg_integer()
               , reqs_counter              :: non_neg_integer()
               , sched_mod                 :: module()
               , sched_state               :: any()
               , try_fire_timeout          :: pos_integer()
               , failure_counter_threshold :: non_neg_integer()
               , trace                     :: [term()]
               , result                    :: atom()
               , exit_timeout              :: pos_integer()
               , notify_exit               :: none | pid()
               , opts                      :: #fd_opts{}
               }).

-define(INTERNAL_TIMEOUT, 500).

config(Sched, Opts) ->
    SchedMod = firedrill:scheduler_to_module(element(1, Sched)),
    #state{ dist_states = case proplists:get_value(dist_mode, Opts, false) of
                              true -> dict:new();
                              false -> none
                          end
          , busy_counter = 0
          , dequeue_counter = 0
          , failure_counter = 0
          , current_failure_counter = 0
          , kick_counter = 0
          , reqs_counter = 0
          , sched_mod = SchedMod
          , sched_state = SchedMod:init(element(2, Sched))
          , try_fire_timeout = proplists:get_value(try_fire_timeout, Opts, 20)
          , failure_counter_threshold = proplists:get_value(failure_threshold, Opts, 500)
          , trace = []
          , result = none
          , exit_timeout = 100
          , notify_exit = none
          , opts = #fd_opts{ verbose_dequeue =
                                 proplists:get_value(verbose_dequeue, Opts, false)
                           , verbose_final =
                                 proplists:get_value(verbose_final, Opts, false)
                           }
          }.

warning(no_fd_sup) ->
    io:format(user, "[FD] fd_sup is unusable~n", []);
warning(ignored_local_idle) ->
    io:format(user, "[FD] ignored local idle message~n", []);
warning(ignored_external_timeout) ->
    io:format(user, "[FD] ignored external timeout~n", []).
warning(scheduler_exit_with_error, E) ->
    io:format(user, "[FD] fd_scheduler exit with error ~p~n", [E]).

start_entry(SchedMod, Opts) ->
    UseNotify =
        case proplists:get_value(use_fd_sup, Opts, true) of
            true ->
                case proplists:get_value(dist_mode, Opts, false) of
                    false ->
                        case (catch fd_sup:notify_begin()) of
                            true ->
                                true;
                            _ ->
                                warning(no_fd_sup),
                                false
                        end;
                    _ -> false
                end;
            _ -> false
        end,
    State = config(SchedMod, Opts),
    Result = (catch dispatcher(State)),
    case UseNotify of
        true ->
            catch fd_sup:notify_end();
        false -> ok
    end,
    case Result of
        #state{} ->
            if
                Result#state.opts#fd_opts.verbose_final orelse Result#state.result =/= exited_by_request ->
                    io:format(user, "[FD] final state = ~p~n", [state_to_readable(Result)]);
                true ->
                    ok
            end,
            catch naive_dispatcher(Result),
            case Result#state.notify_exit of
                none -> ok;
                Pid -> Pid ! {fd_scheduler_exit, Result}
            end;
        E ->
            %% XXX there could be some message hold in Result. Anything can fix?
            catch naive_dispatcher(config(?MODULE, none)),
            warning(scheduler_exit_with_error, E)
    end.

naive_dispatcher() ->
    receive
        #fd_delay_req{ref = Ref, from = From} ->
            From ! #fd_delay_resp{ref = Ref},
            naive_dispatcher();
        _ ->
            naive_dispatcher()
    after 0 -> done
    end.

naive_dispatcher(#state{sched_state = []}) ->
    naive_dispatcher();
naive_dispatcher(#state{sched_state = [#fd_delay_req{ref = Ref, from = From}|T]} = State) ->
    From ! #fd_delay_resp{ref = Ref},
    naive_dispatcher(State#state{sched_state = T}).

dispatcher(State) ->
    receive
        {slave_register, Node, Slave} ->
            case State#state.dist_states of
                none -> dispatcher(State);
                Map ->
                    NewMap = dict:store(Node, #dist_state{slave = Slave}, Map),
                    #state{busy_counter = BC} = State,
                    dispatcher(State#state{dist_states = NewMap, busy_counter = BC + 1})
            end;
        {slave, _Node, M} ->
            case State#state.dist_states of
                none ->
                    dispatcher(State);
                _Map ->
                    case M of
                        exit ->
                            dispatcher(State);
                        {exit, _} ->
                            dispatcher(State);
                        fd_notify_idle ->
                            %% {NewState, ToNotify} =
                            %%     case dict:find(Node, Map) of
                            %%         {ok, _} ->
                            %%             {State#state{busy_counter = State#state.busy_counter - 1},
                            %%              State#state.busy_counter =< 1};
                            %%         _ ->
                            %%             {State, false}
                            %%     end,
                            %% case ToNotify of
                            %%     true ->
                            %%         dispatcher(NewState, fd_notify_idle);
                            %%     false ->
                            %%         dispatcher(NewState)
                            %% end;
                            %%% Currently fd_sup won't help distributed testing,
                            %%% due to uncontrolled external behavior (such as messaging).
                            dispatcher(State);
                        fd_notify_idle_ext ->
                            %% ignored for the same reason
                            dispatcher(State);
                        _ -> dispatcher(State, M)
                    end
            end;
        M ->
            case State#state.dist_states of
                none ->
                    dispatcher(State, M);
                _ ->
                    case M of
                        exit -> dispatcher(State, M);
                        {exit, _} -> dispatcher(State, M);
                        _ -> dispatcher(State)
                    end
            end
    after
        State#state.try_fire_timeout ->
            case State#state.dist_states of
                none ->
                    dispatcher(State, timeout);
                _ ->
                    dispatcher(State#state{busy_counter = 0}, timeout)
            end
    end.

dispatcher(State, M) ->
    %% For debugging ...
    %% if
    %%     M =/= fd_notify_idle andalso M =/= fd_notify_idle_ext ->
    %%         io:format(user, "!!! ~p~n", [M]);
    %%     M =:= fd_notify_idle ->
    %%         io:format(user, "!!! ~p~n", [M]);
    %%     true -> ok
    %% end,
    case M of
        #fd_delay_req{} = Req ->
            buffer_req(State, Req);
        fd_notify_idle ->
            KC = State#state.kick_counter,
            try_fire_req(State#state{kick_counter = KC + 1}, true);
        fd_notify_idle_ext ->
            KC = State#state.kick_counter,
            try_fire_req(State#state{kick_counter = KC + 1}, true);
        {hint, H} ->
            take_hint(State, H);
        exit ->
            exit(State, exited_by_request, none);
        {exit, Pid} ->
            exit(State, exited_by_request, Pid);
        timeout ->
            try_fire_req(State, false);
        {ping, Pid, Ref} ->
            Pid ! {pong, Ref},
            dispatcher(State);
        {log_format, F, A} ->
            io:format(user, "[FD] log: ~s~n", [io_lib:format(F, A)]),
            dispatcher(State);
        _ ->
            dispatcher(State)
    end.

buffer_req(#state{sched_mod = Mod, sched_state = SchedState, reqs_counter = MC} = State, ReqInfo) ->
    {Resp, NewSchedState} = Mod:enqueue_req(ReqInfo, SchedState),
    case Resp of
        ok -> dispatcher(State#state{sched_state = NewSchedState, reqs_counter = MC + 1});
        rejected ->
            #fd_delay_req{ref = Ref, from = From} = ReqInfo,
            From ! #fd_delay_resp{ref = Ref},
            dispatcher(State#state{sched_state = NewSchedState})
    end.

try_fire_req(#state{sched_mod = Mod, sched_state = SchedState, reqs_counter = MC} = State, AllowNone) ->
    case if
             MC =< 0 -> {none, SchedState};
             true -> Mod:dequeue_req(SchedState)
         end of
        {ok, #fd_delay_req{ref = Ref, from = From} = Req, NewSchedState} ->
            case State of
                #state{opts = #fd_opts{verbose_dequeue = true}} ->
                    io:format(user, "[FD] dequeue ~p from ~p requests ~n", [Req, State#state.reqs_counter]);
                _ -> ok
            end,
            From ! #fd_delay_resp{ref = Ref},
            #state{dequeue_counter = DC, trace = Trace} = State,
            case State#state.dist_states of
                none ->
                    dispatcher(State#state{
                                 dequeue_counter = DC + 1, trace = [Req|Trace], reqs_counter = MC - 1, sched_state = NewSchedState, current_failure_counter = 0});
                _ ->
                    dispatcher(State#state{
                                 busy_counter = 1,
                                 dequeue_counter = DC + 1, trace = [Req|Trace], reqs_counter = MC - 1, sched_state = NewSchedState, current_failure_counter = 0})
            end;
        {none, NewSchedState} when not AllowNone ->
            #state{failure_counter = FC, current_failure_counter = CFC, failure_counter_threshold = FCT} = State,
            if
                CFC < FCT ->
                    dispatcher(State#state{sched_state = NewSchedState,
                                           failure_counter = FC + 1,
                                           current_failure_counter = CFC + 1});
                true ->
                    finalize(State#state{result = too_many_failures})
            end;
        {none, NewSchedState} ->
            dispatcher(State#state{sched_state = NewSchedState})
    end.

take_hint(#state{sched_mod = Mod, sched_state = SchedState} = State, Hint) ->
    NewSchedState = Mod:hint(Hint, SchedState),
    dispatcher(State#state{sched_state = NewSchedState}).

exit(State, Reason, NotifyTo) ->
    finalize(State#state{result = Reason, notify_exit = NotifyTo}).

stop(Dispatcher) ->
    Dispatcher ! {exit, self()},
    receive {fd_scheduler_exit, State} -> {State#state.result, State#state.trace} end.

finalize(#state{dist_states = none, sched_mod = Mod} = State) ->
    State#state{sched_state = Mod:to_req_list(State#state.sched_state)};
finalize(#state{dist_states = Map, sched_mod = Mod} = State) ->
    %% Tell all slaves to exit. This doesn't handle corner cases, but I guess this is fine ...
    dict:fold(fun (Node, #dist_state{slave = Slave}, _) ->
                      MRef = monitor(process, Slave),
                      Slave ! {fd_sched_master, exit},
                      receive
                          {'DOWN', MRef, _, _, _} -> ok
                      after
                          ?INTERNAL_TIMEOUT ->
                              io:format(user, "Cannot hear back from slave node ~p pid ~p~n", [Node, Slave])
                      end,
                      ok
              end, ok, Map),
    State#state{dist_states = dict:to_list(State#state.dist_states),
                sched_state = Mod:to_req_list(State#state.sched_state)}.

state_to_readable(#state{reqs_counter = ReqCounter, dequeue_counter = DequeueCounter, failure_counter = FailureCounter, kick_counter = KickCounter}) ->
    #{remaining_cnt => ReqCounter, dequeue_cnt => DequeueCounter, failure_cnt => FailureCounter, kick_cnt => KickCounter}.

%%% scheduler process running on slave nodes

-record(slave_state, {master :: node()}).

start_slave_entry(Master, Opts) ->
    State = #slave_state{master = Master},
    Master ! {slave_register, node(), self()},
    UseNotify =
        case proplists:get_value(use_fd_sup, Opts, true) of
            true ->
                case (catch fd_sup:notify_begin()) of
                    true ->
                        true;
                    _ ->
                        warning(no_fd_sup),
                        false
                end;
            _ -> false
        end,
    case (catch slave_dispatcher(State)) of
        #slave_state{} -> ok;
        E -> io:format(user, "slave dispatcher error ~p~n", [E])
    end,
    case UseNotify of
        true ->
            catch fd_sup:notify_end();
        false ->
            ok
    end,
    catch fd_scheduler:naive_dispatcher(),
    %% do not need to send back as now we use 'monitor'
    % Master ! {slave_exited, node(), self()},
    ok.

slave_dispatcher(#slave_state{master = Master} = State) ->
    receive
        {fd_sched_master, exit} ->
            State;
        Else ->
            Master ! {slave, node(), Else},
            slave_dispatcher(State)
    end.

%%% Naive FIFO scheduler callbacks

init(_) ->
    queue:new().

enqueue_req(ReqInfo, Reqs) ->
    {ok, queue:in(ReqInfo, Reqs)}.

dequeue_req(Reqs) ->
    %% this cannot fail
    {{value, ReqInfo}, NewReqs} = queue:out(Reqs),
    {ok, ReqInfo, NewReqs}.

hint(_, Reqs) ->
    Reqs.

to_req_list(Reqs) ->
    queue:to_list(Reqs).
