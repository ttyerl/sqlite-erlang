ERL=erl
OTP_TOP=/opt/local/lib/erlang/lib
PLT_SRC=$(OTP_TOP)/kernel-2.12.4/ebin $(OTP_TOP)/stdlib-1.15.4/ebin/ $(OTP_TOP)/crypto-1.5.2.1/ebin $(OTP_TOP)/compiler-4.5.4/ebin $(OTP_TOP)/hipe-3.6.8/ebin/ ebin

all: compile docs

compile:
	$(ERL) -make
	cd priv && make

static:
	dialyzer --build_plt --output_plt sqlite.plt -r ebin $(PLT_SRC)

clean:
	- rm ebin/*.beam 
	- rm doc/*
	- rm -rf ct_nodes* all_runs.html variables*
	find . -name "*~" | xargs rm
	cd priv && make clean

docs:
	$(ERL) -noshell -run edoc_run application "'sqlite'" '"."' '[{title,"Welcome to sqlite"},{hidden,false},{private,false}]' -s erlang halt
