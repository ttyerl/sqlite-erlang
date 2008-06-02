#ifndef _ERL_COMM_H
#define _ERL_COMM_H
typedef unsigned char byte;

int read_cmd(byte *buf);
int write_cmd(byte *buf, int len);

#endif 
