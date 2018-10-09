-module(fd_cth).

-export([id/1
        ,init/2
        ,pre_init_per_suite/3
        ,post_init_per_suite/4
        ,pre_end_per_suite/3
        ,post_end_per_suite/4
        ,pre_init_per_group/4
        ,post_init_per_group/5
        ,pre_end_per_group/4
        ,post_end_per_group/5
        ,pre_init_per_testcase/4
        ,post_init_per_testcase/5
        ,pre_end_per_testcase/4
        ,post_end_per_testcase/5
        ,on_tc_fail/4
        ,on_tc_skip/4
        ,terminate/1]).

-record(state, {opts :: term()
               ,dump_trace :: boolean()
               ,dump_trace_on_tc_failure :: boolean()}).

id(_) ->
    ?MODULE.

init(_, Opts) ->
    #state{opts = Opts,
           dump_trace = false,
           dump_trace_on_tc_failure = proplists:get_value(dump_trace_on_tc_failure, Opts, true)}.

pre_init_per_suite(_Suite, Config, State) ->
    firedrill:start(State#state.opts),
    {Config, State}.

post_init_per_suite(_Suite, _Config, Return, State) ->
    {Return, State}.

pre_end_per_suite(_Suite, Config, State) ->
    {Config, State}.

post_end_per_suite(_Suite, _Config, Return, State) ->
    {_, Trace} = firedrill:stop(),
    case State of
        #state{dump_trace = true} ->
            %% didn't find a way to write to ct logs. ct:pal doesn't work ...
            io:format(user, "FireDrill Trace = ~p~n", [Trace]);
        _ -> ok
    end,
    {Return, State}.

pre_init_per_group(_Suite, _Group, Config, State) ->
    {Config, State}.

post_init_per_group(_Suite, _Group, _Config, Return, State) ->
    {Return, State}.

pre_end_per_group(_Suite, _Group, Config, State) ->
    {Config, State}.

post_end_per_group(_Suite, _Group, _Config, Return, State) ->
    {Return, State}.

pre_init_per_testcase(_Suite, _TC, Config, State) ->
    {Config, State}.

post_init_per_testcase(_Suite, _TC, _Config, Return, State) ->
    {Return, State}.

pre_end_per_testcase( _Suite, _TC, Config, State) ->
    {Config, State}.

post_end_per_testcase(_Suite, _TC, _Config, Return, State) ->
    {Return, State}.

on_tc_fail(_Suite, _TC, _Reason, State) ->
    State#state{dump_trace = State#state.dump_trace_on_tc_failure}.

on_tc_skip(_Suite, _TC, _Reason, State) ->
    State.

terminate(_State) ->
    ok.
