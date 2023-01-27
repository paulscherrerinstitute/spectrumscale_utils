# spectrumscale_utils
A small set of tools for exploring usage etc on Spectrum Scale filesystems, mostly loading information into Pandas Dataframes.

- `get_data_from_mmrepquota`: Reads the output of "mmrepquota -Y" and creates a dictionary of DataFrames, indexed by the groupby argument
- `get_timeseries_from_mmrepquota`: Creates a pandas timeseries from a directory containing outputs of mmrepquota command
- `get_timeseries_from_policy`: Get a pandas DataFrame from a list policy scan 
- `get_data_from_iohist`: Get data from `mmdiag --iohist`

see each method documentation for more info.

