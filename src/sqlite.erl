%%%-------------------------------------------------------------------
%%% File    : sqlite.erl
%%% Author  : Tee Teoh <tteoh@tee-teohs-macbook.local>
%%% Description : 
%%%
%%% Created : 31 May 2008 by Tee Teoh <tteoh@tee-teohs-macbook.local>
%%%-------------------------------------------------------------------
-module(sqlite).

-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([stop/1, close/1]).
-export([sql_exec/2]).

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
    start_link(Db, [{db, "./" ++ atom_to_list(Db) ++ ".db"}]).

%%--------------------------------------------------------------------
%% Function: start_link(Db, Options) -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server. 
%%               {db, DbFile :: String()}
%%--------------------------------------------------------------------
start_link(Db, Options) ->
    gen_server:start_link({local, Db}, ?MODULE, [], Options).

close(Db) ->
    gen_server:call(Db, close).

stop(Db) ->
    close(Db).
    
sql_exec(Db, SQL) ->
    gen_server:call(Db, {sql_exec, SQL}).
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
    Dbase = proplist:get_value(db, Options),
    Port = open_port({spawn, create_cmd(Dbase)
    "sqlite_port " ++ Dbase.
