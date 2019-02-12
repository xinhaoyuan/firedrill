-module(fd_rand_helper).

-export([new_priority/2]).

new_priority(#{delay_level := Level}, Rng) ->
    {P, NewRng} = rand:uniform_s(Rng),
    {P - Level, NewRng};
new_priority(#{weight := Weight}, Rng) ->
    {UP, NewRng} = rand:uniform_s(Rng),
    {1 - math:pow(UP, 1/Weight), NewRng};
new_priority(_, Rng) ->
    {UP, NewRng} = rand:uniform_s(Rng),
    {UP, NewRng}.
