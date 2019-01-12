-module(firedrill).

-include("firedrill.hrl").

-export([get_module_opts/2, start/1, stop/0, delay/3, apply/4, send/2, send/3, ping_scheduler/0]).

-export([debug_log_format/2]).

-define(NAME_SCHED, fd_sched).
-define(NAME_MASTER, fd_sched_master).

get_module_opts(rw, Opts) ->
    {fd_sched_rw, Opts};
get_module_opts(basicpos, Opts) ->
    {fd_sched_basicpos, Opts};
get_module_opts(pos, Opts) ->
    {fd_sched_pos, Opts};
get_module_opts(lazypos, Opts) ->
    {fd_sched_pos, [{variant, lazy} | Opts]};
get_module_opts(rapos, Opts) ->
    {fd_sched_rapos, Opts};
get_module_opts(sampling, Opts) ->
    {fd_sched_sampling, Opts};
get_module_opts({module, M}, Opts) ->
    {M, Opts};
get_module_opts(_, _) ->
    {fd_scheduler, []}.

start(Opts) ->
    Master = proplists:get_value(master, Opts, none),
    Sched = proplists:get_value(scheduler, Opts, {naive, []}),
    Me = self(),
    if
        Master =:= none orelse Master =:= node() ->
            {NewOpts, SchedulerName} =
                case Master of
                    none -> {proplists:delete(dist_mode, Opts), ?NAME_SCHED};
                    _ -> {[dist_mode|proplists:delete(dist_mode, Opts)], ?NAME_MASTER}
                end,
            spawn(fun () ->
                          case (catch register(SchedulerName, self())) of
                              true ->
                                  case proplists:get_value(verbose_debug, NewOpts, false) of
                                      true -> io:format(user, "[FD] ~p started~n", [SchedulerName]);
                                      false -> ok
                                  end,
                                  Me ! fd_scheduler_start,
                                  fd_scheduler:start_entry(Sched, NewOpts),
                                  case proplists:get_value(verbose_debug, NewOpts, false) of
                                      true -> io:format(user, "[FD] ~p ended~n", [SchedulerName]);
                                      false -> ok
                                  end;
                              _ ->
                                  case proplists:get_value(verbose_debug, NewOpts, false) of
                                      true -> io:format(user, "!!! failed to register ~p !!!~n", [SchedulerName]);
                                      false -> ok
                                  end,
                                  %% TODO: return error back to print out stack trace?
                                  Me ! fd_scheduler_start
                              end
                      end),
            receive fd_scheduler_start -> ok end;
        true ->
            case rpc:call(Master, erlang, whereis, [?NAME_MASTER]) of
                {badrpc, _} -> none;
                undefined -> none;
                MasterPid ->
                    spawn(fun () ->
                                  case (catch register(?NAME_SCHED, self())) of
                                      true ->
                                          case proplists:get_value(verbose_debug, Opts, false) of
                                              true -> io:format(user, "[FD] slave started~n", []);
                                              false -> ok
                                          end,
                                          Me ! fd_scheduler_start,
                                          fd_scheduler:start_slave_entry(MasterPid, Opts),
                                          case proplists:get_value(verbose_debug, Opts, false) of
                                              true -> io:format(user, "[FD] slave ended~n", []);
                                              false -> ok
                                          end;
                                      _ ->
                                          case proplists:get_value(verbose_debug, Opts, false) of
                                              true -> io:format(user, "!!! failed to register fd_sched !!!~n", []);
                                              false -> ok
                                          end
                                  end
                          end),
                    receive fd_scheduler_start -> ok end,
                    ok
            end
    end.

stop() ->
    %% No need to unregister since the process terminates
    case whereis(?NAME_MASTER) of
        undefined ->
            fd_scheduler:stop(?NAME_SCHED);
        Pid ->
            fd_scheduler:stop(Pid)
    end.

send(To, Msg) ->
    delay(To, message, Msg),
    To ! Msg.

send(To, Msg, _) ->
    %% options are ignored
    send(To, Msg),
    ok.

apply({ets, EtsTab}, Mod, Func, Args) ->
    To = if
             is_atom(EtsTab) ->
                 %% ets:whereis is not available ...
                 EtsTab;
             true -> EtsTab
         end,
    delay({object, To}, apply, {Mod, Func, Args}),
    erlang:apply(Mod, Func, Args);
apply({node, To}, Mod, Func, Args) ->
    delay(To, apply, {Mod, Func, Args}),
    erlang:apply(Mod, Func, Args);
apply({raw_fn_arg, Pos}, Mod, Func, Args) ->
    if
        is_integer(Pos), 0 < Pos, Pos =< length(Args) ->
            delay({object, lists:nth(Pos, Args)}, apply, {Mod, Func, Args});
        true ->
            delay({object, undefined}, apply, {Mod, Func, Args})
    end,
    erlang:apply(Mod, Func, Args).

delay(To, Type, Data) when is_pid(To) orelse is_port(To) ->
    case whereis(?NAME_SCHED) of
        undefined -> ok;
        Pid ->
            internal_delay(Pid, To, Type, Data)
    end;
delay({object, To}, Type, Data) ->
    case whereis(?NAME_SCHED) of
        undefined ->
            ok;
        Pid ->
            internal_delay(Pid, To, Type, Data)
    end;
delay(To, Type, Data) when is_atom(To) ->
    ToPid = whereis(To),
    case whereis(?NAME_SCHED) of
        undefined -> ok;
        Pid ->
            internal_delay(Pid, ToPid, Type, Data)
    end;
delay({Node, Name}, Type, Data) when is_atom(Node) andalso is_atom(Name) ->
    case whereis(?NAME_SCHED) of
        undefined -> ok;
        SchedPid ->
            case rpc:call(Node, erlang, whereis, [Name]) of
                {badrpc, _} -> ok;
                ToPid -> internal_delay(SchedPid, ToPid, Type, Data)
            end
    end;
delay(_, _, _) ->
    ok.

internal_delay(SchedPid, To, Type, Data) ->
    MRef = make_ref(),
    SchedPid ! #fd_delay_req{ref = MRef, from = self(), to = To, type = Type, data = Data},
    receive #fd_delay_resp{ref = MRef} -> ok end.

ping_scheduler() ->
    case whereis(?NAME_SCHED) of
        undefined ->
            undefined;
        Pid ->
            MRef = monitor(process, Pid),
            Pid ! {ping, self(), MRef},
            receive
                {pong, MRef} ->
                    demonitor(MRef, [flush]),
                    pong;
                {'DOWN', MRef, _, _, _} ->
                    undefined
            end
    end.

debug_log_format(F, A) ->
    case whereis(?NAME_SCHED) of
        undefined ->
            io:format(user, "[FD] detached log: ~s", [io_lib:format(F, A)]);
        Pid ->
            Pid ! {log_format, F, A}
    end.
