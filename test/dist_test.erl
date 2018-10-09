-module(dist_test).
-include("firedrill.hrl").
-include_lib("eunit/include/eunit.hrl").

simple_test() ->
    Master = start_slave(fd_master),
    FDOpts = [{master, Master}, verbose_final],
    rpc:call(Master, firedrill, start, [FDOpts]),
    firedrill:start(FDOpts),
    firedrill:delay({object, none}, message, []),
    io:format(user, "firedrill:stop returns ~p~n", [rpc:call(Master, firedrill, stop, [])]),
    ok.

%%% Fragment copied from gproc tests

start_slaves(Ns) ->
    [H|T] = Nodes = [start_slave(N) || N <- Ns],
    _ = [rpc:call(H, net_adm, ping, [N]) || N <- T],
    Nodes.

start_slave(Name) ->
    case node() of
        nonode@nohost ->
            os:cmd("epmd -daemon"),
            {ok, _} = net_kernel:start([main, shortnames]);
        _ ->
            ok
    end,
    {Pa, Pz} = paths(),
    Paths = "-pa ./ -pz ../ebin" ++
        lists:flatten([[" -pa " ++ Path || Path <- Pa],
		       [" -pz " ++ Path || Path <- Pz]]),
    {ok, Node} = slave:start_link(host(), Name, Paths),
    Node.

paths() ->
    Path = code:get_path(),
    {ok, [[Root]]} = init:get_argument(root),
    {Pas, Rest} = lists:splitwith(fun(P) ->
					  not lists:prefix(Root, P)
				  end, Path),
    {_, Pzs} = lists:splitwith(fun(P) ->
				       lists:prefix(Root, P)
			       end, Rest),
    {Pas, Pzs}.


host() ->
    [_Name, Host] = re:split(atom_to_list(node()), "@", [{return, list}]),
    list_to_atom(Host).
