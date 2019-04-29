.PHONY: compile rel test typecheck ci

REBAR=./rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

cover:
	$(REBAR) cover

test: compile
	$(REBAR) as test do eunit,ct

typecheck:
	$(REBAR) dialyzer

ci:
	$(REBAR) dialyzer && $(REBAR) as test do eunit,ct,cover
	$(REBAR) covertool generate
	codecov -f _build/test/covertool/relcast.covertool.xml
