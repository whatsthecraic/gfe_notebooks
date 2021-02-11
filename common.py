import functools
import glob
import math
import os.path
import pandas as pd
import re
import sqlalchemy as sqla


def import_gfe_dbms(sql_or_table, datasource):
    """
    Load the content of a view or table from the database `datasource' into
    a newly created pandas dataframe
    :param sql_or_table: either a table/view to load or a full SQL command
    :return: a pandas dataframe with the content of the loaded table
    """
    # Parse the datasource file
    if not datasource.endswith('.sqlite3'): datasource += ".sqlite3"
    if not os.path.exists(datasource) and datasource.find('/') == -1:
        datasource = 'data/' + datasource
    if not os.path.exists(datasource):
        raise FileNotFoundError("The file `" + datasource + "' does not exist")

    db = sqla.create_engine("sqlite:///" + datasource)

    # connection = db.connect()
    sql_string = sql_or_table.lower() # convert to lower case
    if(not sql_string.startswith("select") and not sql_string.startswith("with")):
        sql_string = "SELECT * FROM " + sql_string # the argument is simply a table or a view
    df = pd.read_sql(sql_string, db)

    # replace the column client_graph (full path to the graph) with simply the graph name
    if "client_graph" in df.columns:
        def get_graph_name(absolute_path):
            filename = os.path.basename(absolute_path)
            # remove the extension
            if filename.endswith('properties'): filename = os.path.splitext(filename)[0]
            return filename
        df["graph"] = df["client_graph"].apply(get_graph_name)
        df["graph"] = df["graph"].apply(lambda x: re.sub(r'-dense$', '', x)) # remove the suffix -dense
        # replace client_graph with graph
        indexOf = df.columns.to_list().index("client_graph")
        df.drop(["client_graph"], axis=1, inplace=True)
        df = df[ df.columns[:indexOf].to_list() + ["graph"] + df.columns[indexOf: -1].to_list() ]

    # replace the measured times with a TimeDelta (depending on the column suffix)
    units = {
        "seconds": ["sec", "secs", "second", "seconds"],
        "milliseconds": ["millisec", "millisecs", "millisecond", "milliseconds"],
        "microseconds": ["usec", "usecs", "microsec", "microsecs", "microsecond", "microseconds"]
    }
    for column in df.columns:
        column = str(column)
        indexOf = column.rfind("_")
        if indexOf == -1: continue
        suffix = column[indexOf +1:].lower()
        unit = None
        for u in units:
            for c in units[u]:
                if suffix == c:
                    unit = u
                    break
        if unit is not None:
            df[column] = df[column].apply(lambda x: pd.to_timedelta(x, unit=unit) if x != 0 else pd.NaT)
            df.rename(columns={column: column[:indexOf]}, inplace=True)

    # add a convenience column in plain secs, to ease aggregation
    columns = df.columns; # original columns
    for index, column in reversed(list(enumerate( df.columns ))):
        if pd.api.types.is_timedelta64_dtype( df[column] ):
            df[column + "_secs"] = df[column].apply(lambda x: x.total_seconds()) # create the new column
            columns = columns.insert(index +1, column + "_secs") # reposition the new column just after the previous one
    df = df[ columns ] # reorder the columns        
            
    # connection.close()
    return df


def import_gfe(sql_or_table):
    """
    From 15/Oct/2021, we use again only a single database to store all results

    :param sql_or_table: the SQL query to execute over the databases
    :return: a new dataframe, representing the result of the query
    """
    return import_gfe_dbms(sql_or_table, "data/data21.sqlite3")


# def import_gfe(sql_or_table):
#     """
#     Execute the query against all databases in the directory data/data* and report a new
#     data frame with the concatenation of all result sets obtained
#
#     :param sql_or_table: the SQL query to execute over the databases
#     :return: a new dataframe, representing the result of the query
#     """
#     list_data_frames = []
#     for database in glob.glob('data/data*.sqlite3'):
#         df = import_gfe_dbms(sql_or_table, database)
#         database = os.path.basename(database).replace('.sqlite3', '')
#         df.insert(0, "database", database) # add an attribute `database' with the name of the database
#         list_data_frames.append(df)
#
#     result = pd.concat(list_data_frames, ignore_index=True)
#     return result


def import_graphmat(path_csv = 'data/graphmat/results.csv'):
    """
    Retrieve a dataframe with the results from graphmat, as stored in results_graphmat.csv
    :return: a DataFrame with the results from graphmat
    """
    graphmat_csv = pd.read_csv(path_csv)
    graphmat = pd.concat( [
        graphmat_csv.iloc[:, :-3],
        graphmat_csv.iloc[:, -3:].applymap(lambda x: pd.to_timedelta(x, 'seconds')) ],
    axis="columns")

    # t_startup_pec is the percentage of t_makespan - t_processing
    graphmat["t_startup_perc"] = ( graphmat["t_makespan"] - graphmat["t_processing"] ) / graphmat["t_makespan"] * 100.0
    graphmat.sort_values(["algorithm", "graph"], inplace=True)

    return graphmat


def prepare_barchart(df, col_x_axis, col_group, col_y_axis):
    """

    Example: prepare_barchart("graph", "library", "median")

    :param df:
    :param col_x_axis: the column for the x axis (e.g. "graph")
    :param col_group: the attribute with the groups (e.g. "library")
    :param col_y_axis: the column fo the y axis is the actual measurement (e.g. completion_time)
    :return: a pandas DataFrame with the above specified format
    """
    df = df.copy() # silent the warning SettingWithCopy

    convert_to_timedelta = False
    df["_ct"] = df.loc[:, col_y_axis]
    try:
        df["_ct"] = df["_ct"].apply(lambda x: x.total_seconds())
        convert_to_timedelta = True
    except AttributeError:
        pass

    agg = df.groupby([col_x_axis, col_group]).agg(
        time = pd.NamedAgg(
            column="_ct", aggfunc="median"
        )
    )
    df.drop("_ct", axis=1, inplace=True)
    agg.reset_index(inplace=True)

    tbl_final = None
    for group_name in agg[col_group].unique():
        # select the relevant data
        tbl = agg[agg[col_group] == group_name][[col_x_axis, "time"]]
        tbl.set_index(col_x_axis, inplace=True)
        if convert_to_timedelta:
            tbl = tbl.apply(lambda x: pd.to_timedelta(x, unit="seconds"))
        tbl.rename({"time": group_name}, axis=1, inplace=True)
        tbl_final = pd.DataFrame(tbl) if tbl_final is None else pd.concat([tbl_final, tbl], axis=1, sort=True)

    # sort the attributes / libraries
    tbl_final = tbl_final[ sorted(tbl_final.columns.to_list()) ]

    return tbl_final


def aging_medians(df = None):
    '''
    Return for each library, graph and parallelism degree, the execution (exec_id) that accomplished the median throughput. The
    exec_id can be further used to pick the execution to portray in the plot for the throughput over time.
    
    :param df: an instance of view_updates_throughput, properly filtered
    :return: a table with the median throughput of each execution
    '''
    if df is None:
        df = import_gfe("view_updates")

    # compute the median of each group
    def compute_median(group):
        num_samples = len(group)
        df = group.sort_values("throughput")
        df = df.reset_index(drop=True)
        df = df.loc[ math.floor( num_samples / 2 ) ];
        df["count"] = num_samples
        df["mem_gb"] = round( df["memory_footprint_bytes"] / 1024 / 1024 / 1024, 2);
        df = df[["exec_id", "throughput", "mem_gb", "completion_time", "count", "timeout_hit"]]
        return df

    return df.groupby(["aging", "library", "graph", "num_threads"]).apply(compute_median)


def aging_execid_progress(df):
    '''
    Return for each library, graph and parallelism degree, the execution (exec_id) that accomplished the average execution time
    in view_updates_progress

    :param df: an instance of view_updates_progress, properly filtered
    :return: a matrix where the rows are pair <library, graph>, the columns the parallelism degree, and the component is exec_id with the median execution time
    '''

    df = df.copy() # silent the warning SettingWithCopy
    m = df["progress"].max() # max aging coefficient
    medians = df[(df["aging"] == m) & (df["progress"] == m)].\
        groupby(["library", "graph", "num_threads"]).\
        aggregate(completion_time=pd.NamedAgg(
            column='completion_time',
            aggfunc=functools.partial(pd.Series.quantile, interpolation='nearest')
    ))

    join = pd.merge(medians, df) # library, graph, num_threads and completion_time are the only columns in common

    # in case there are multiple exec_id with the same execution time, select only one, the one with the min exec_id
    join = join.groupby(["library", "graph", "num_threads"]).agg(exec_id=('exec_id', 'min')).reset_index()

    # Use fill_value="NaN" to avoid converting everything to fload and obtaining decimal IDs such as 1234.0
    return join.pivot_table(index=("library", "graph"), columns="num_threads", values="exec_id", fill_value="NaN")


def edges_per_graph():
    '''
    Retrieve the number of vertices and edges in each graph evaluated
    '''
    
    data = pd.DataFrame({
        "graph": ["dota-league", "graph500-22", "uniform-22", "graph500-24", "uniform-24", "graph500-25", "uniform-25", "graph500-26", "uniform-26"],
        "num_vertices": [61170, 2396657, 2396657, 8870942, 8870942, 17062472, 17062472, 32804978, 32804978],
        "num_edges": [50870313, 64155735, 64155735, 260379520, 260379520, 523602831, 523602831, 1051922853, 1051922853]
    })
    data = data.set_index("graph")
    return data


def fmtlabel(value):
    '''
    The numeric label to be shown at the top of a bar chart
    '''
    if(value >= 10 ** 9):
        return "{:.2f} G".format(value / 10**9)
    elif(value >= 10 ** 6):
        return "{:.2f} M".format(value / 10**6)
    elif(value >= 10 ** 3):
        return "{:.2f} k".format(value / 10**3)
    else:
        return "{:.2f}".format(value)