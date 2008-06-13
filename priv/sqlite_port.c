#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <string.h>

#include "erl_comm.h"
#include "erl_interface.h"
#include "ei.h"

#define MASTER_QUERY "select * from sqlite_master where type='table';"

static FILE *log;

static ETERM *result;

void respond(ETERM *r);
void send_error(char *err_msg);
void send_result();
void send_ok();

// 4 sql = CREATE TABLE t1 (t1key INTEGER PRIMARY KEY, data TEXT, num double, timeEnter DATE)
static int list_tables(void *notUsed, int argc, char **argv, char **azColName) 
{
  if (result == 0) {
    result = erl_mk_empty_list();
  }
  
  fprintf(log, "%d %s = %s\n", 2, azColName[2], argv[2]);
  fprintf(log, "\n");
  fflush(log);

  result = erl_cons(erl_mk_atom(argv[2]), result);

  return 0;
}

static int callback(void *notUsed, int argc, char **argv, char **azColName) 
{
  ETERM **record_list;
  int i;

  if (result == 0) {
    result = erl_mk_empty_list();
  }

  record_list = malloc(argc * sizeof(ETERM *));
  
  fprintf(log, "runs %d\n", argc);
  for (i = 0; i < argc; i++) {
    fprintf(log, "%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    if (argv[i]) {
      record_list[i] = erl_mk_string(argv[i]);
    }
    else {
      record_list[i] = erl_mk_empty_list();
    }
  }
  fprintf(log, "\n");
  fflush(log);

  result = erl_cons(erl_mk_tuple(record_list, argc), result);

  free(record_list);
  return 0;
}

int main(int argc, char **argv) 
{
  ETERM *tuplep;
  ETERM *fnp, *argp;
  byte buf[1024];

  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;

  log = fopen("/tmp/sqlite_port.log", "a+");
  fprintf(log, "******start log (%s)******\n", argv[1]);
  fflush(log);

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
      fprintf(log, "closing sqlite3_close\n");
      fflush(log);

      sqlite3_close(db);
      break;
    }
    else if (strncmp((const char *)ERL_ATOM_PTR(fnp), "list_tables", 11) == 0) {
      fprintf(log, "calling list_tables\n");

      result = 0;

      rc = sqlite3_exec(db, MASTER_QUERY, list_tables, 0, &zErrMsg);
      if (rc != SQLITE_OK) {
	send_error(zErrMsg);
	sqlite3_free(zErrMsg);
      }
      else if (result != 0)  {
	send_result();
      }
      else {
	// not an error and no results. still need to return something
	send_ok();
      } 

      fflush(log);

    }
    else if (strncmp((const char *)ERL_ATOM_PTR(fnp), "sql_exec", 8) == 0) {
      fprintf(log, "calling sqlite3_exec %s\n", erl_iolist_to_string(argp));

      result = 0;

      rc = sqlite3_exec(db, erl_iolist_to_string(argp), callback, 0, &zErrMsg);
      if (rc != SQLITE_OK) {
	send_error(zErrMsg);
	sqlite3_free(zErrMsg);
      }
      else if (result != 0)  {
	send_result();
      }
      else {
	// not an error and no results. still need to return something
	send_ok();
      } 

      fflush(log);
    }

    erl_free_compound(tuplep);
    erl_free_term(fnp);
    erl_free_term(argp);
  }

  fprintf(log, "******end log******\n");
  fclose(log);
  return 0;
}
      
void send_error(char *err_msg) 
{
  ETERM *tup_list[2];
  ETERM *to_send;
	
  tup_list[0] = erl_mk_atom("sql_error");
  tup_list[1] = erl_mk_string(err_msg);
  to_send = erl_mk_tuple(tup_list, 2);

  fprintf(log, "SQL Error: %s\n", err_msg);
  respond(to_send);

  erl_free_term(tup_list[0]);
  erl_free_term(tup_list[1]);
  erl_free_compound(to_send);
}

void send_result() 
{
  fprintf(log, "returning at len %d\n", erl_term_len(result));
  respond(result);
	
  erl_free_compound(result);
  result = 0;
}

void send_ok() 
{
  ETERM *to_send;

  to_send = erl_mk_atom("ok");
  fprintf(log, "returning ok at len %d\n", erl_term_len(to_send));
  respond(to_send);
  	
  erl_free_term(to_send);
}

void respond(ETERM *r) 
{
  byte buf[2048];
  bzero(buf, 2048);
  erl_encode(r, buf);
  write_cmd(buf, erl_term_len(r));

  fprintf(log, "sending response back\n");
}
