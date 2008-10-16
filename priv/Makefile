OTP_DIR=/opt/local/lib/erlang/lib/erl_interface-3.5.5.3
LIB=-lsqlite3 -lerl_interface -lei
FLAGS=-I$(OTP_DIR)/include -L$(OTP_DIR)/lib
GCC=/usr/bin/gcc

all: obj
	$(GCC) -Wall erl_comm.o sqlite_port.o $(FLAGS) $(LIB) -o sqlite_port

obj: erl_comm.c sqlite_port.c
	$(GCC) -Wall -c $(FLAGS) erl_comm.c sqlite_port.c 

clean:
	- rm -f *.o sqlite_port

