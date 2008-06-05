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
-export([open/1, open/2]).
-export([start_link/1, start_link/2]).
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
    gen_server:start_link({local, Db}, ?MODULE, Options, []).

open(Db) ->
    ?MODULE:start_link(Db).

open(Db, Options) ->
    ?MODULE:start_link(Db, Options).

close(Db) ->
    gen_server:call(Db, close).

stop(Db) ->
    ?MODULE:close(Db).
    
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
    port_command(Port, term_to_binary({sql_exec, SQL})),
    Reply = receive 
		{Port, {data, Data}} when is_binary(Data) ->
		    List = binary_to_term(Data),
		    if is_list(List) ->
			    lists:reverse(List);
		       true -> List
		    end;
		_ ->
		    ok
	    end,
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


