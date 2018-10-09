-module(fd_dist_sim).

%%% This module is currently DEPRECATED!

%%% Simulate a distributed erlang environment for testing.
%%% It uses group_leader to identify simulated nodes.
%%%
%%% Note that this is not a full simulation of distributed environment!

-export([ global_init/0
        , sim_node/0, sim_node/1, sim_node_init_loop/1
        , create_node/1, sim_spawn/4, sim_spawn_link/4
        , sim_call/4, sim_group_leader/2
        , sim_register/2, sim_unregister/1
        , sim_whereis/1, sim_whereis/2
        ]).

-export([ node/0
        , register/2, unregister/1
        , spawn/4, spawn_link/4
        , whereis/1, whereis/2
        ]).

-define(TAB, fd_dist_sim_tab).

global_init() ->
    ets:new(?TAB, [named_table]),
    ets:insert(?TAB, [{erlang:group_leader(), erlang:node()}, {erlang:node(), erlang:group_leader()}]).

sim_node() ->
    sim_node(self()).

sim_node(Pid) ->
    {group_leader, GL} = process_info(Pid, group_leader),
    R = ets:lookup(?TAB, GL),
    case R of
        [] ->
            case GL of
                Pid -> error(fd_dist_sim__node_self_loop);
                _ -> sim_node(GL)
            end;
        [{_, Node}] ->
            Node
    end.

sim_node_init_loop(G) ->
    receive M ->
            G ! M,
            sim_node_init_loop(G)
    end.

create_node(Node) when is_atom(Node) ->
    G = group_leader(),
    SimInit = erlang:spawn(fun () -> sim_node_init_loop(G) end),
    case ets:insert_new(?TAB, [{Node, SimInit}, {SimInit, Node}]) of
        true -> {true, SimInit};
        _ -> exit(SimInit, kill), false
    end.

sim_spawn(Node, Mod, Func, Args) when is_atom(Node) ->
    R = ets:lookup(?TAB, Node),
    case R of
        [] ->
            spawn(fun () -> ok end);
        [{_, Init}] ->
            spawn(fun () ->
                          erlang:group_leader(Init, self()),
                          apply(Mod, Func, Args)
                  end)
    end.

sim_spawn_link(Node, Mod, Func, Args) when is_atom(Node) ->
    R = ets:lookup(?TAB, Node),
    case R of
        [] ->
            spawn_link(fun () -> ok end);
        [{_, Init}] ->
            spawn_link(fun () ->
                               erlang:group_leader(Init, self()),
                               apply(Mod, Func, Args)
                       end)
    end.

sim_call(Node, Mod, Func, Args) when is_atom(Node) ->
    R = ets:lookup(?TAB, Node),
    case R of
        [] -> {badrpc, noconnection};
        [{_, Init}] ->
            Me = self(),
            Ref = make_ref(),
            spawn_link(fun () ->
                               erlang:group_leader(Init, self()),
                               Me ! {rpc_resp, Ref, apply(Mod, Func, Args)}
                       end),
            receive
                {rpc_resp, Ref, R} -> R
            end
    end.

sim_group_leader(LG, Pid) ->
    {group_leader, OldLG} = process_info(Pid, group_leader),
    case sim_node(OldLG) =:= sim_node(LG) of
        true ->
            erlang:group_leader(LG, pid);
        false ->
            error(fd_dist_sim__set_remote_group_leader)
    end.

sim_register(Name, Pid) ->
    Node = sim_node(Pid),
    ets:insert_new(?TAB, {{Node, Name}, Pid}).

sim_unregister(Name) ->
    Node = sim_node(self()),
    ets:delete(?TAB, {Node, Name}),
    %% Error detection?
    true.

sim_whereis(Name) ->
    Node = sim_node(self()),
    sim_whereis(Node, Name).

sim_whereis(Node, Name) ->
    R = ets:lookup(?TAB, {Node, Name}),
    case R of
        [] ->
            io:format(user, "[FD] whereis is unable to resolve name ~p of node ~p, stacktrace = ~p~n",
                      [Node, Name, erlang:get_stacktrace()]),
            undefined;
        [{_, Pid}] ->
            Pid
    end.

node() ->
    sim_node().

register(Name, Pid) ->
    sim_register(Name, Pid).

unregister(Name) ->
    sim_unregister(Name).

spawn(Node, Mod, Func, Args) ->
    sim_spawn(Node, Mod, Func, Args).

spawn_link(Node, Mod, Func, Args) ->
    sim_spawn_link(Node, Mod, Func, Args).

whereis(Name) ->
    sim_whereis(Name).

whereis(Node, Name) ->
    sim_whereis(Node, Name).
