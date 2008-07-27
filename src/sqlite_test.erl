%%%-------------------------------------------------------------------
%%% File    : sqlite_test.erl
%%% Author  : Tee Teoh <tteoh@teemac.ott.cti.com>
%%% Description : 
%%%
%%% Created : 10 Jun 2008 by Tee Teoh <tteoh@teemac.ott.cti.com>
%%%-------------------------------------------------------------------
-module(sqlite_test).

%% API
-export([create_table_test/0]).

-record(user, {name, age, wage}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: 
%% Description:
%%--------------------------------------------------------------------
create_table_test() ->
    sqlite:open(ct),
    sqlite:create_table(ct, user, [{name, text}, {age, integer}, {wage, integer}]),
    [user] = sqlite:list_tables(ct),
    [{name, text}, {age, integer}, {wage, integer}] = sqlite:table_info(ct, user),
    sqlite:write(ct, user, [{name, "abby"}, {age, 20}, {wage, 2000}]),
    sqlite:write(ct, user, [{name, "marge"}, {age, 30}, {wage, 3000}]),
    sqlite:sql_exec(ct, "select * from user;"),
    sqlite:read(ct, user, {name, "abby"}),
    sqlite:delete(ct, user, {name, "abby"}),
    sqlite:drop_table(ct, user),
%sqlite:delete_db(ct)
    sqlite:close(ct).
% create, read, update, delete
%%====================================================================
%% Internal functions
%%====================================================================
