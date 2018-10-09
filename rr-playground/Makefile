.PHONY: all libs

ERL_TEST_SRC := $(wildcard ../test/*.erl)
ERL_TEST_BEAM := $(patsubst ../test/%.erl,ebin/%.beam,${ERL_TEST_SRC})

ebin/%.beam: ../test/%.erl
	erlc -pa ../_build/default/lib/parse_trans/ebin \
	     -pa ../_build/default/lib/firedrill/ebin \
	     -DTEST -I ../include -o ebin $<

all: libs ebin ${ERL_TEST_BEAM}

libs:
	cd ..; rebar3 compile

ebin:
	mkdir -p $@

test1: ebin/rr_test1.beam
	erl -noshell -pa ../_build/default/lib/firedrill/ebin -pa ebin -eval "eunit:test(rr_test1, [verbose])" -s init stop

test1-record: ebin/rr_test1.beam
	rr record -M erl -noshell -pa ../_build/default/lib/firedrill/ebin -pa ebin -eval "eunit:test(rr_test1, [verbose])" -s init stop

test2: ebin/dist_test.beam
	erl -noshell -pa ../_build/default/lib/firedrill/ebin -pa ebin -eval "eunit:test(dist_test, [verbose])" -s init stop

test2-record: ebin/dist_test.beam
	rr record -M erl -noshell -pa ../_build/default/lib/firedrill/ebin -pa ebin -eval "eunit:test(dist_test, [verbose])" -s init stop
