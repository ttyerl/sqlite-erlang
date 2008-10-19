%%%-------------------------------------------------------------------
%%% File    : sqlite_test_SUITE.erl
%%% Author  : Tee Teoh <tteoh@teemac.ott.cti.com>
%%% Description : 
%%%
%%% Created :  4 Sep 2008 by Tee Teoh <tteoh@teemac.ott.cti.com>
%%%-------------------------------------------------------------------
-module(sqlite_test_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("ct.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{minutes,10}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    %%[{dbase, userct} | Config].
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    sqlite:open(TestCase),
    [{dbase, TestCase} | Config].

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(TestCase, _Config) ->
    sqlite:close(TestCase),
    ok.

%%--------------------------------------------------------------------
%% Function: sequences() -> Sequences
%%
%% Sequences = [{SeqName,TestCases}]
%% SeqName = atom()
%%   Name of a sequence.
%% TestCases = [atom()]
%%   List of test cases that are part of the sequence
%%
%% Description: Specifies test case sequences.
%%--------------------------------------------------------------------
sequences() -> 
    [].

%%--------------------------------------------------------------------
%% Function: all() -> TestCases | {skip,Reason}
%%
%% TestCases = [TestCase | {sequence,SeqName}]
%% TestCase = atom()
%%   Name of a test case.
%% SeqName = atom()
%%   Name of a test case sequence.
%% Reason = term()
%%   The reason for skipping all test cases.
%%
%% Description: Returns the list of test cases that are to be executed.
%%--------------------------------------------------------------------
all() -> 
    [create_table].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: TestCase() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Test case info function - returns list of tuples to set
%%              properties for the test case.
%%
%% Note: This function is only meant to be used to return a list of
%% values, not perform any other operations.
%%--------------------------------------------------------------------
create_table() -> 
    [].

%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%% Comment = term()
%%   A comment about the test case that will be printed in the html log.
%%
%% Description: Test case function. (The name of it must be specified in
%%              the all/0 list for the test case to be executed).
%%--------------------------------------------------------------------
create_table(Config) -> 
    Dbase = proplists:get_value(dbase, Config),
    sqlite:create_table(Dbase, user, [{name, text}, {age, integer}, {wage, integer}]),
    [user] = sqlite:list_tables(Dbase),
    [{name, text}, {age, integer}, {wage, integer}] = sqlite:table_info(Dbase, user),
    sqlite:write(Dbase, user, [{name, "abby"}, {age, 20}, {wage, 2000}]),
    sqlite:write(Dbase, user, [{name, "marge"}, {age, 30}, {wage, 3000}]),
    [{"abby","20","2000"},{"marge","30","3000"}] = sqlite:sql_exec(Dbase, "select * from user;"),
    [{"abby","20","2000"}] = sqlite:read(Dbase, user, {name, "abby"}),
    sqlite:delete(Dbase, user, {name, "abby"}),
    sqlite:drop_table(Dbase, user),
    ok.
