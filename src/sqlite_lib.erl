%%%-------------------------------------------------------------------
%%% File    : sqlite_lib.erl
%%% Author  : Tee Teoh 
%%% Description : 
%%%
%%% Created : 21 Jun 2008 by Tee Teoh 
%%%-------------------------------------------------------------------
-module(sqlite_lib).

%% API
-export([col_type/1]).
-export([write_value_sql/1, write_col_sql/1]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: col_type(Type :: term()) -> term()
%% Description: Maps sqlite column type.
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
%% Function: write_value_sql(Value :: [term()]) -> string()
%% Description: Creates the values portion of the sql stmt.
%%              Currently only support integer, double/float and strings.
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
%% Function: write_col_sql([atom()]) -> string()
%% Description: Creates the column/data stmt for SQL.
%%--------------------------------------------------------------------
write_col_sql(Cols) ->
    StrCols = lists:map(fun(X) ->
				atom_to_list(X)
			end, Cols),
    string:join(StrCols, ",").


%%====================================================================
%% Internal functions
%%====================================================================
