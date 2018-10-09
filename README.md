Firedrill
=====

Randomized concurrency testing for Erlang.

This library uses the randomized scheduling algorithms introduced in our publication:

  Xinhao Yuan, Junfeng Yang, and Ronghui Gu. 2018. Partial Order Aware Concurrency Sampling.
  In Computer Aided Verification - 30th International Conference, CAV 2018.

Workflow
-----

1. Compile the SUT with instrumentation that gives control to Firedrill on synchronization points.

2. Initialize/finalize `fd_scheduler` before/after tests.

3. Keep runing your tests repeatly to reveal potential concurrency bugs, if there is any.

Limitations
----

 - It alone cannot determinisitcally reproduce any bugs found. See Morpheus Integration.

 - No guarantee of verification. This is NOT a model checking approach.

 - Testing in distributed setting has considerable overhead. Try to support single node testing in SUT if possible.

Usage and Examples
-----

See the unit testcases in `test/` for basic examples.

Build
-----

    $ rebar3 compile

Morpheus Integration
-----

TODO - Documentation

License
----

APL 2.0 -- See LICENSE

