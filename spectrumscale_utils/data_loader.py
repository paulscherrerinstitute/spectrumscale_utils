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
    df["blockUsage"] = pd.to_numeric(df["blockUsage"]) / 1e6
    filesystems = df[groupby].unique()
    data_usage_group = {}

    for fs in filesystems:
        data_usage_group[fs] = df[df[groupby] == fs].set_index("name")

    return data_usage_group


def get_timeseries_from_mmrepquota(datadir, quantity="blockUsage", groupby="filesetname", points_per_dir=1, fname="mmrepquota-j.txt"):
    """Creates a pandas timeseries from a directory containing outputs of mmrepquota command. It is assuming the following 
    directory structure: <datadir>/<date>/<hour>/fname, e.g. usage/2018-01-01/00/mmrepquota-g.txt.
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
    fname : str, optional
        name of the file containing the information from mmrepquota -g or -j

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
            df_t = get_data_from_mmrepquota(datadir + "/" + d + "/" + hour + "/mmrepquota-j.txt", groupby=groupby)
    
            if df_t is None:
                continue
    
            # this assumes no new filesets are created over time
            #if dfs == {}:
            #    dfs = dict([(k, pd.DataFrame()) for k in df_t.keys()])
    
            for k in df_t.keys():
                # skip root fileset, as it has the same name 
                if k in ["COMMON", "root"]:
                    continue
                
                cname = d + " %02d:00:00" % int(hour)
                serie = df_t[k][quantity]
                serie.name = cname
                if k not in dfs:
                    dfs[k] = serie 
                else:
                #    print("-------------- {}".format(k))
                #    print(dfs[k])
                #    print(serie)
                    #return dfs, serie
                    #try:
                    dfs[k] = pd.concat([dfs[k][~dfs[k].index.duplicated()], serie[~serie.index.duplicated()]], axis=1, sort=True)
                    #except:
                    #    print(dfs, dfs[k][dfs[k].index.duplicated()])
                    #    return
                #print(k, ",", dfs[k][cname].sum(), df_t[k][quantity].sum())

            points += 1
    
            if points_per_dir <= points:
                break
    
    for k in dfs.keys():
        dfs[k].columns = pd.to_datetime(dfs[k].columns)
        dfs[k] = dfs[k].T
        dfs[k].sort_index(inplace=True)
        dfs[k].index = pd.to_datetime(dfs[k].index)
    
    print("Read %d points" % points)
    return dfs
    
# def get_timeseries_from_mmrepquota(datadir, quantity="blockUsage", groupby="filesetname", points_per_dir=1):
#     """Creates a pandas timeseries from a directory containing outputs of mmrepquota command. It is assuming the following 
#     directory structure: <datadir>/<date>/<hour>/mmrepquota-g.txt, e.g. usage/2018-01-01/00/mmrepquota-g.txt.

#     It assumes no new filesets or filesystems are created over time

#     It returns a dictionary of DataFrames, indexed using the groupby argument
    
#     Parameters
#     ----------
#     datadir : str
#         directory containing the required output files from Spectrum Scale
#     quantity : str, optional
#         which quantity to use in the DataFrame (the default is "blockUsage", which counts used space)
#     groupby : str, optional
#         on which field to index (the default is "filesetname", it can also be filesystemName)
#     points_per_dir : int, optional
#         how many data points per date to use (the default is 1). This depends on the directory structure described above
    
#     Returns
#     -------
#     dict of Dataframes
#         dictionary of DataFrames, indexed using the groupby argument
#     """

#     dfs = {}
#     points = 0
#     dates = [i for i in os.listdir(datadir)]

#     for d in dates:
#         for hour in os.listdir(datadir + "/" + d):
#             df_t = get_data_from_mmrepquota(datadir + "/" + d + "/" + hour + "/mmrepquota-g.txt", groupby=groupby)
    
#             if df_t is None:
#                 continue
    
#             # this assumes no new filesets are created over time
#             if dfs == {}:
#                 dfs = dict([(k, pd.DataFrame()) for k in df_t.keys()])
    
#             for k in df_t.keys():
#                 # skip root fileset, as it has the same name 
#                 if k == "root":
#                     continue
                
#                 cname = d + " %d:00:00" % int(hour)
#                 serie = df_t[k][quantity]
#                 serie.name = cname
#                 if k not in dfs:
#                     dfs[k] = serie 
#                 else:
#                     dfs[k] = pd.concat([dfs[k], serie], axis=1, sort=True)
#                 #print(k, ",", dfs[k][cname].sum(), df_t[k][quantity].sum())

#             points += 1
    
#             if points_per_dir <= points:
#                 break
    
#     for k in dfs.keys():
#         dfs[k].columns = pd.to_datetime(dfs[k].columns)
#         dfs[k] = dfs[k].T
#         dfs[k].sort_index(inplace=True)
    
#     print("Read %d points" % points)
#     return dfs


def get_timeseries_from_policy(f, header, index_date="CREATION_DATE", drop_separators=True, encoding="unicode_escape", nrows=None):
    """Get a pandas DataFrame from a policy scan such as:

    RULE 'listall' list 'all-files'  
    SHOW( varchar(kb_allocated) || ' * ' || varchar(file_size) || ' * ' || varchar(user_id) || ' * ' || fileset_name || ' * ' || varchar(creation_time) )

    
    Parameters
    ----------
    f : str
        file containing the policy output
    
    Returns
    -------
    pandas DataFrame
    """

    headers = ["Inode number", "gen number", "Snapshot ID", ]
    seps = []
    i = 0
    for i, h in enumerate(header):
        headers.append(h)
        if h.find("DATE") != -1:
            headers.append(h.replace("DATE", "TIME"))
        seps.append("sep%d" % i)
        headers.append("sep%d" % i)

    #headers += ["kb_allocated", "sep1", "filesize", "sep2", "user_id", "sep3", "fileset_name", "sep4", "creation_date", "creation_time"]
    headers += ["Filename"]

    dates = [h for h in headers if h.find("_DATE") != -1]
    df = pd.read_csv(f, sep=r"\s+", names=headers, encoding=encoding, error_bad_lines=False, nrows=nrows)

    # this is not terribly efficient, it is done to deal with some peculiarities of policy output format (and take into account of
    # weird chars in filenames, e.g. commas)
    for h in dates:
        df[h] = df[h] + " " + df[h.replace("DATE", "TIME")]
        df[h] = pd.to_datetime(df[h])
        df.drop(h.replace("DATE", "TIME"), inplace=True, axis=1)
    #df["date"] = df[index_date + "_DATE"] + " " + df[index_date + "_TIME"]
    #df["date"] = pd.to_datetime(df["date"])
    df2 = df.set_index(index_date)
    df2.sort_index(inplace=True)
    if drop_separators:
        for sep in seps:
            df2.drop(sep, inplace=True, axis=1)
    return df2


def get_data_from_iohist(f, verbose_iohist=False):
    header = ["RW", "buf type", "disk:sec", "nSec", "time_ms", "type", "NSD ID", "NSD node"]
    if verbose_iohist:
        header += ["info1", "info2", "context", "thread"]
    df = pd.read_csv(f, skiprows=7, sep=r"\s*", names=header, engine="python")
    return df
