# Background

Our database includes time series data for several hundred  different signalsacross a dozen or so data sources. Each individual time series dataset stored in the database is uniquely identified by its (source, signal) pair.  The datasets differ significantly in size and complexity even though they have the same basic structure.

We have an analytics job that runs each night to compute statistics on each time series dataset. The job works through each (source, signal) pair using a thread pool.  When the job begins, it logs the size of the thread pool which is currently configured.  While the job is running, it logs the start time of computation for each pair. When the job is complete, it logs some summary information.

# Scenario
In staging, this analytics job takes an hour to complete in production, it occasionally takes the expected hour, but usually takes between four and five hours.

The staging database was cloned from a relatively recent backup of prod. So it’s not a difference-of-scale problem

You want to see if you can tease out any useful information from the existing logs before exploring more-detailed profiling options.  If there is a pattern in which pairs are taking longer than expected to analyze, it could inform your next step


# Meta Log Analyzer

The meta_log_analyzer is build on overall job running a thread pool with 10 thread running concurrently.
The analyzer pulls in data from the input folder and creates a dataframe and a csv file for further EDA.
Next, the dataframe is separated by thread ( ten in total ) and sorted by thread and ordered by timestamp.
From here focusing on a thread, we can calculate the runtime using the time difference of the start time of a
(source,signal) pair and the consecutive (source,signal) pair on the same thread.
The run_time was calculated using for n using X(n) - X(n+1) which is actually the runtime of X(n-1),
so in order to correct this the column was shifted up one row.
Continuing from here we can calculate the following
`
 - the total wall-clock running time
 - The pair that took the longest
 - All pairs that took longer than 10 minutes 
`


# Running instructions

Please create a results folder the same level as the input folder.
Please ensure the input folder has the metadata_update.log ready for analysis.
The result folder will contain the generated report and a csv file for visualization and Exploratory Data Analysis
`
python3 meta_log_analyzer.py
`
# Sample output


The total wall-clock running time
************************************************************
0 days 04:08:55.968095

The pair that took the longest
************************************************************
(chng, smoothed_adj_outpatient_cli) 0 days 04:08:55.964634

All pairs that took longer than 10 minutes
************************************************************
(chng, smoothed_adj_outpatient_covid) 0 days 00:10:24.751151
  (chng, smoothed_adj_outpatient_cli) 0 days 04:08:55.964634
    (chng, smoothed_outpatient_covid) 0 days 04:08:55.924849

