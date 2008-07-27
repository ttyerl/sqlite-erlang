%%%-------------------------------------------------------------------
%%% File    : sqlite_lib.erl
%%% @author Tee Teoh
%%% @copyright 21 Jun 2008 by Tee Teoh 
%%% @version 1.0.0
%%% @doc Library module for sqlite
%%% @end
%%%-------------------------------------------------------------------
-module(sqlite_lib).

%% API
-export([col_type/1]).
-export([write_value_sql/1, write_col_sql/1]).
-export([create_table_sql/2, write_sql/2, read_sql/3, delete_sql/3, drop_table/1]). 

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec col_type(Type :: term()) -> term()
%% @doc Maps sqlite column type.
%%--------------------------------------------------------------------
col_type(integer) ->
    "INTEGER";     
col_type("INTEGER") ->
    integer;
col_type(text) ->
    "TEXT";
col_type("TEXT") ->
    text;
col_type(double) ->
    "DOUBLE";
col_type("DOUBLE") ->
    double;
col_type(date) ->
    "DATE";
col_type("DATE") ->
    date.

%%--------------------------------------------------------------------
%% @spec write_value_sql(Value :: [term()]) -> string()
%% @doc 
%%    Creates the values portion of the sql stmt.
%%    Currently only support integer, double/float and strings.
%% @end
%%--------------------------------------------------------------------
write_value_sql(Values) ->
    StrValues = lists:map(fun(X) when is_integer(X) ->
				  integer_to_list(X);
			     (X) when is_float(X) ->
				  float_to_list(X);
			     (X) ->
				  io_lib:format("'~s'", [X])
			  end, Values),
    string:join(StrValues, ",").

%%--------------------------------------------------------------------
%% @spec write_col_sql([atom()]) -> string()
%% @doc Creates the column/data stmt for SQL.
%%--------------------------------------------------------------------
write_col_sql(Cols) ->
    StrCols = lists:map(fun(X) ->
				atom_to_list(X)
			end, Cols),
    string:join(StrCols, ",").

%%--------------------------------------------------------------------
%% @spec create_table_sql(Tbl, [{ColName, Type}]) -> string()
%%       Tbl = atom()
%%       ColName = atom()
%%       Type = string()
%% @doc Generates a table create stmt in SQL.
%%--------------------------------------------------------------------
create_table_sql(Tbl, [{ColName, Type} | Tl]) ->
    CT = io_lib:format("CREATE TABLE ~p ", [Tbl]),
    Start = io_lib:format("(~p ~s PRIMARY KEY, ", [ColName, sqlite_lib:col_type(Type)]),
    End = string:join(
	    lists:map(fun({Name0, Type0}) ->
			      io_lib:format("~p ~s", [Name0, sqlite_lib:col_type(Type0)])
		      end, Tl), ", ") ++ ");",
    lists:flatten(CT ++ Start ++ End).

%%--------------------------------------------------------------------
%% @spec write_sql(Tbl, Data) -> string()
%%       Tbl = atom()
%%       Data = [{ColName :: atom(), Values :: string() | integer() | float()}]
%% @doc Taking Data as list of column names and values pairs it creates the
%%      proper insertion SQL stmt.
%% @end
%%--------------------------------------------------------------------
write_sql(Tbl, Data) ->
    {Cols, Values} = lists:unzip(Data),
    lists:flatten(
      io_lib:format("INSERT INTO ~p (~s) values (~s);", 
		    [Tbl, 
		     sqlite_lib:write_col_sql(Cols), 
		     sqlite_lib:write_value_sql(Values)])).

%%--------------------------------------------------------------------
%% @spec read_sql(Tbl, Key, Value) -> string()
%%       Tbl = atom()
%%       Key = atom()
%%       Value = string() | integer() | float()
%% @doc Using Key as the column name searches for the record with
%%      matching Value.
%% @end
%%--------------------------------------------------------------------
read_sql(Tbl, Key, Value) ->
    lists:flatten(
      io_lib:format("SELECT * FROM ~p WHERE ~p = ~p;", [Tbl, Key, Value])).

%%--------------------------------------------------------------------
%% @spec delete_sql(Tbl, Key, Value) -> string()
%%       Tbl = atom()
%%       Key = atom()
%%       Value = string() | integer() | float()
%% @doc Using Key as the column name searches for the record with
%%      matching Value then deletes that record.
%% @end
%%--------------------------------------------------------------------
delete_sql(Tbl, Key, Value) ->
    lists:flatten(
      io_lib:format("DELETE FROM ~p WHERE ~p = ~p;", [Tbl, Key, Value])).

%%--------------------------------------------------------------------
%% @spec drop_table(Tbl) -> string()
%%       Tbl = atom()
%% @doc Drop the table Tbl from the database
%% @end
%%--------------------------------------------------------------------
drop_table(Tbl) ->
    lists:flatten(
      io_lib:format("DROP TABLE ~p;", [Tbl])).

%%====================================================================
%% Internal functions
%%====================================================================
