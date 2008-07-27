ERL=erl

all: compile docs

compile:
	$(ERL) -make
	cd priv && make

clean:
	- rm ebin/*.beam 
	- rm src/*
	find . -name "*~" | xargs rm
	cd priv && make clean

docs:
	$(ERL) -noshell -run edoc_run application "'sqlite'" '"."' '[{title,"Welcome to sqlite"},{hidden,false},{private,false}]' -s erlang halt
