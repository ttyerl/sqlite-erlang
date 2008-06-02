OTP_DIR=/opt/local/lib/erlang/lib/erl_interface-3.5.5.3
LIB=-lsqlite3 -lerl_interface -lei
FLAGS=-I$(OTP_DIR)/include -L$(OTP_DIR)/lib
GCC=/opt/local/bin/gcc-apple-4.2 

all: erl_comm.c sqlite_port.c
	$(GCC) -Wall -o sqlite_port erl_comm.c sqlite_port.c $(FLAGS) $(LIB)

obj: erl_comm.c sqlite_port.c
	$(GCC) -Wall -c $(FLAGS) erl_comm.c sqlite_port.c

clean:
	- rm -f *.o sqlite_port
