%%%-------------------------------------------------------------------
%%% File    : sqlite.erl
%%% Author  : Tee Teoh
%%% Description : 
%%%
%%% Created : 31 May 2008 by Tee Teoh
%%%-------------------------------------------------------------------
-module(sqlite).

-behaviour(gen_server).

%% API
-export([open/1, open/2]).
-export([start_link/1, start_link/2]).
-export([stop/0, close/1]).
-export([sql_exec/1, sql_exec/2]).

-export([create_table/2, create_table/3]).
-export([list_tables/0, list_tables/1, table_info/1, table_info/2]).
-export([write/2, write/3]).

-export([write_sql/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {port, ops = []}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link(Db) -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Db) ->
    ?MODULE:open(?MODULE, [{db, "./" ++ atom_to_list(Db) ++ ".db"}]).

%%--------------------------------------------------------------------
%% Function: start_link(Db, Options) -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server. 
%%               {db, DbFile :: String()}
%%--------------------------------------------------------------------
start_link(Db, Options) ->
    Opts = case proplists:get_value(db, Options) of
	       undefined -> [{db, "./" ++ atom_to_list(Db) ++ ".db"} | Options];
	       _ -> Options
	   end,
    ?MODULE:open(?MODULE, Opts).

open(Db) ->
    ?MODULE:open(Db, [{db, "./" ++ atom_to_list(Db) ++ ".db"}]).

open(Db, Options) ->
    gen_server:start_link({local, Db}, ?MODULE, Options, []).


close(Db) ->
    gen_server:call(Db, close).

stop() ->
    ?MODULE:close(?MODULE).
    
sql_exec(SQL) ->
    ?MODULE:sql_exec(?MODULE, SQL).

sql_exec(Db, SQL) ->
    gen_server:call(Db, {sql_exec, SQL}).

create_table(Tbl, Options) ->
    ?MODULE:create_table(?MODULE, Tbl, Options).

create_table(Db, Tbl, Options) ->
    gen_server:call(Db, {create_table, Tbl, Options}).

% returns list or ok
list_tables() ->
    ?MODULE:list_tables(?MODULE).

list_tables(Db) ->
    gen_server:call(Db, list_tables).

table_info(Tbl) ->
    ?MODULE:table_info(?MODULE, Tbl).

table_info(Db, Tbl) ->
    gen_server:call(Db, {table_info, Tbl}).

write(Tbl, Data) ->
    ?MODULE:write(?MODULE, Tbl, Data).

write(Db, Tbl, Data) ->
    gen_server:call(Db, {write, Tbl, Data}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(Options) ->
    Dbase = proplists:get_value(db, Options),
    Port = open_port({spawn, create_cmd(Dbase)}, [{packet, 2}, binary]),
    {ok, #state{port = Port, ops = Options}}.
		     
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(close, _From, State) ->
    Reply = ok,
    {stop, normal, Reply, State};
handle_call({sql_exec, SQL}, _From, #state{port = Port} = State) ->
    Reply = exec(Port, {sql_exec, SQL}),
    {reply, Reply, State};
handle_call(list_tables, _From, #state{port = Port} = State) ->
    Reply = exec(Port, {list_tables, none}),
    {reply, Reply, State};
handle_call({table_info, Tbl}, _From, #state{port = Port} = State) ->
    % make sure we only get table info.
    % SQL Injection warning
    SQL = io_lib:format("select sql from sqlite_master where tbl_name = '~p' and type='table';", [Tbl]),
    [{Info}] = exec(Port, {sql_exec, SQL}),
    Reply = parse_table_info(Info),
    {reply, Reply, State};
handle_call({create_table, Tbl, Options}, _From, #state{port = Port} = State) ->
    SQL = create_table_sql(Tbl, Options),
    Cmd = {sql_exec, SQL},
    Reply = exec(Port, Cmd),
    {reply, Reply, State};
handle_call({write, Tbl, Data}, _From, #state{port = Port} = State) ->
    % insert into t1 (data,num) values ('This is sample data',3);
    Reply = exec(Port, {sql_exec, write_sql(Tbl, Data)}),
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(normal, #state{port = Port}) ->
    port_command(Port, term_to_binary({close, nop})),
    port_close(Port),
    ok;
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

create_cmd(Dbase) ->
    "sqlite_port " ++ Dbase.

exec(Port, Cmd) ->
    port_command(Port, term_to_binary(Cmd)),
    receive 
	{Port, {data, Data}} when is_binary(Data) ->
	    List = binary_to_term(Data),
	    if is_list(List) ->
		    lists:reverse(List);
	       true -> List
	    end;
	_ ->
	    ok
    end.


parse_table_info(Info) ->
    [_, Tail] = string:tokens(Info, "()"),
    Cols = string:tokens(Tail, ","), 
    build_table_info(lists:map(fun(X) ->
				       string:tokens(X, " ") 
			       end, Cols), []).
   
build_table_info([], Acc) -> 
    lists:reverse(Acc);
build_table_info([[ColName, ColType] | Tl], Acc) -> 
    build_table_info(Tl, [{list_to_atom(ColName), sqlite_lib:col_type(ColType)}| Acc]); 
build_table_info([[ColName, ColType, "PRIMARY", "KEY"] | Tl], Acc) ->
    build_table_info(Tl, [{list_to_atom(ColName), sqlite_lib:col_type(ColType)}| Acc]).
    
create_table_sql(Tbl, [{Name, Type} | Tl]) ->
    CT = io_lib:format("CREATE TABLE ~p ", [Tbl]),
    Start = io_lib:format("(~p ~s PRIMARY KEY, ", [Name, sqlite_lib:col_type(Type)]),
    End = string:join(
	    lists:map(fun({Name0, Type0}) ->
			      io_lib:format("~p ~s", [Name0, sqlite_lib:col_type(Type0)])
		      end, Tl), ", ") ++ ");",
    lists:flatten(CT ++ Start ++ End).

% insert into t1 (data,num) values ('This is sample data',3);
write_sql(Tbl, Data) ->
    {Cols, Values} = lists:unzip(Data),
    lists:flatten(
      io_lib:format("INSERT INTO ~p (~s) values (~s);", 
		    [Tbl, sqlite_lib:write_col_sql(Cols), sqlite_lib:write_value_sql(Values)])).


