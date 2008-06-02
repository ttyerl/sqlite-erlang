#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <string.h>

#include "erl_comm.h"
#include "erl_interface.h"
#include "ei.h"

int main(int argc, char **argv) 
{
  ETERM *tuplep, *intp;
  ETERM *fnp, *argp;
  int res;
  byte buf[100];
  long allocated, freed;

  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;

  rc = sqlite3_open(argv[1], &db);
  if (rc) {
    sqlite3_close(db);
    exit(1);
  }

  erl_init(NULL, 0);

  while (read_cmd(buf) > 0) {
    tuplep = erl_decode(buf);
    fnp = erl_element(1, tuplep);
    argp = erl_element(2, tuplep);
    
    if (strncmp((const char *)ERL_ATOM_PTR(fnp), "close", 5) == 0) {
      sqlite3_close(db);
      break;
    } else if (strncmp((const char *)ERL_ATOM_PTR(fnp), "bar", 3) == 0) {
      res = ERL_INT_VALUE(argp);
    }

    intp = erl_mk_int(res);
    erl_encode(intp, buf);
    write_cmd(buf, erl_term_len(intp));

    erl_free_compound(tuplep);
    erl_free_term(fnp);
    erl_free_term(argp);
    erl_free_term(intp);
  }

  return 0;
}
      


static int callback(void *notUsed, int argc, char **argv, char **azColName) {
	int i;
	printf("runs %d\n", argc);
	for (i = 1; i < argc; i++) {
		printf("%s = %s\n", azColName[i], argv[1] ? argv[1] : "NULL");
	}
	printf("\n");
	return 0;
}

/*
	rc = sqlite3_exec(db, "SELECT * from t1;", callback, 0, &zErrMsg);
	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL Error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	sqlite3_close(db);
*/
