import pandas as pd
import os


def get_data_from_mmrepquota(f, groupby="filesetname"):
    """Reads the output of "mmrepquota -Y" and creates a dictionary of DataFrames, indexed by the groupby argument.
    
    Parameters
    ----------
    f : str
        the file to be read
    groupby : str, optional
        key on which do the indexing (the default is "filesetname", can also be "filesystemName")
    
    Returns
    -------
    dict of DataFrames
    """

    try:
        df = pd.read_csv(f, comment="*", sep=":", )
    except:
        print("Cannot read %s" % f)
        return None

    df.drop(["mmrepquota", "Unnamed: 1", "HEADER", "reserved", "reserved.1"], axis=1, inplace=True)
    df.drop(df[df.version == "version"].index, inplace=True)
    df["blockUsage"] = pd.to_numeric(df["blockUsage"]) / 1e9
    filesystems = df[groupby].unique()
    data_usage_group = {}

    for fs in filesystems:
        data_usage_group[fs] = df[df[groupby] == fs].set_index("name")

    return data_usage_group


def get_timeseries_from_mmrepquota(datadir, quantity="blockUsage", groupby="filesetname", points_per_dir=1):
    """Creates a pandas timeseries from a directory containing outputs of mmrepquota command. It is assuming the following 
    directory structure: <datadir>/<date>/<hour>/mmrepquota-g.txt, e.g. usage/2018-01-01/00/mmrepquota-g.txt.

    It assumes no new filesets or filesystems are created over time

    It returns a dictionary of DataFrames, indexed using the groupby argument
    
    Parameters
    ----------
    datadir : str
        directory containing the required output files from Spectrum Scale
    quantity : str, optional
        which quantity to use in the DataFrame (the default is "blockUsage", which counts used space)
    groupby : str, optional
        on which field to index (the default is "filesetname", it can also be filesystemName)
    points_per_dir : int, optional
        how many data points per date to use (the default is 1). This depends on the directory structure described above
    
    Returns
    -------
    dict of Dataframes
        dictionary of DataFrames, indexed using the groupby argument
    """

    dfs = {}
    points = 0
    dates = [i for i in os.listdir(datadir)]

    for d in dates:
        for hour in os.listdir(datadir + "/" + d):
            df_t = get_data_from_mmrepquota(datadir + "/" + d + "/" + hour + "/mmrepquota-g.txt", groupby=groupby)
    
            if df_t is None:
                continue
    
            # this assumes no new filesets are created over time
            if dfs == {}:
                dfs = dict([(k, pd.DataFrame()) for k in df_t.keys()])
    
            for k in df_t.keys():
                # skip root fileset, as it has the same name 
                if k == "root":
                    continue
                if k not in dfs:
                    dfs[k] = pd.DataFrame()
                cname = d + " %d:00:00" % int(hour)
                dfs[k][cname] = df_t[k][quantity]
    
            points += 1
    
            if points_per_dir <= points:
                break
    
    for k in dfs.keys():
        dfs[k].columns = pd.to_datetime(dfs[k].columns)
        dfs[k] = dfs[k].T
    
    print("Read %d points" % points)
    return dfs
