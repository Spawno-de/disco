-module(disco_counters).

-export([select_job_counters/1, sum_job_counters/1, serialize_job_counters/1, render_job_counters/1]).

%%------------------------------------------------------------------------------
%% Retruns path to file, where counters will be stored.
%%------------------------------------------------------------------------------
job_counters_file_name(JobName) ->
    filename:join(disco:jobhome(JobName), "counters").

%%------------------------------------------------------------------------------
%% gproc:select/3 is rather complecated so I wrap it with select_job_counters/1.
%% It returns all counters in job in format [{CounterName, CounterValue}, ...]
%%------------------------------------------------------------------------------
select_job_counters(JobName) ->
    Key = {JobName, '_', '_'},
    GProcKey = {c, l, Key},
    MatchHead = {GProcKey, '_', '_'},
    Guard = [],
    Result = ['$$'],
    SelectedCounters = gproc:select([{MatchHead, Guard, Result}]),
    lists:map(fun([{c, l, {_JobName, _TaskId, CounterName}}, _Pid, CounterValue]) ->
                      {CounterName, CounterValue} end,
              SelectedCounters).

%%------------------------------------------------------------------------------
%% List returned from select_job_counters/1 may containg duplicates,
%% which are counters from diffrent tasks.
%% It returns all counters in job in format [{CounterName, CounterValue}, ...]
%%------------------------------------------------------------------------------
sum_job_counters(JobCounters) ->
    SearchAndAddFun = fun({CounterName, CounterValue}, CounterList) ->
                              case lists:keyfind(CounterName, 1, CounterList) of
                                  false ->
                                      [{CounterName, CounterValue} | CounterList];
                                  {CounterName, OldValue} ->
                                      [{CounterName, CounterValue + OldValue} | lists:keydelete(CounterName, 1, CounterList)]
                              end
                      end,
    lists:foldl(SearchAndAddFun, [], JobCounters).

%%------------------------------------------------------------------------------
%% Write counter information to file.
%%------------------------------------------------------------------------------
serialize_job_counters(JobName) ->
    {ok, _JobName} = disco:make_dir(disco:jobhome(JobName)),
    {ok, File} = file:open(job_counters_file_name(JobName), [append, raw]),
    CounterData = sum_job_counters(select_job_counters(JobName)),
    AllCounterData = render_job_counters(CounterData),
    file:write(File, AllCounterData),
    file:close(File).

%%------------------------------------------------------------------------------
%% @doc Make string from counter information stored in ETS table.
%%------------------------------------------------------------------------------
render_job_counters(CounterData) ->
    JsonData = lists:map(fun(Counter) ->
                                 {Name, Value} = Counter,
                                 {struct, [{binary_to_list(Name), Value}]}
                         end,
                         CounterData),
    mochijson2:encode(JsonData).
