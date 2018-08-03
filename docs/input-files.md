# Input Files

Several different input file types are required to run this software package.

## Calibration configuration file

The calibration configuration file is the "top level" configuration file:

- This configuration file contains metadata about the analysis to be run, dataset locations and output information.
- The file also contains the location of the correct calibration workflow to be run upon executing the tool.
- The tool is run by invoking `python cmscalibration <calibration-conf.json>`, assuming a valid configuration file at path `<path/calibration-conf.json>`.

The structure can be seen in the following example:

```
{
    "start_date": "2018-01-01",
    "end_date": "2018-01-01",
    "jm_input_dataset": "data/20180625/jm-dataset.json",
    "wm_input_dataset": "data/20180625/wm-dataset.json",
    "node_info": "data/gridka-benchmarks-2017.csv",
    "output_directory": "out",
    "workflow_module": "workflows.sampling_validation"
}

```

## Dataset Configuration File

To be able to handle large datasets, a dataset can be split up into multiple files. In this case, the dataset structure is described in a dataset configuration file. This allows to load only a part of the files of the full dataset, thereby improving performance.

This configuration file includes paths to the single files and dates that denote the time spans of the data included in the respective file.

An example of such a configuration file is shown below:

```
{
    "name": "wmarchive-2018-first-half",
    "files": [
        {
            "file": "201801.txt",
            "start": "2018-01-01",
            "end": "2018-02-01"
        },
        {
            "file": "201802.txt",
            "start": "2018-02-01",
            "end": "2018-03-01"
        },
        {
            "file": "201803.txt",
            "start": "2018-03-01",
            "end": "2018-04-01"
        },
        {
            "file": "201804.txt",
            "start": "2018-04-01",
            "end": "2018-05-01"
        },
        {
            "file": "201805.txt",
            "start": "2018-05-01",
            "end": "2018-06-01"
        },
        {
            "file": "201806.txt",
            "start": "2018-06-01",
            "end": "2018-07-01"
        }
    ]
}
```

## Datasets

The required structure of the datasets depends on the analysis to be run and the type of dataset.

Dataset types include:

- WMArchive job reports (an example of a raw report can be found [here](wmarchive-record-examples/wmarchive-processing.json)) in JSON format
- JobMonitoring job reports in CSV format
- Local performance information, such as
    - Node and performance data
    - CPU efficiency data
    - VO share data (share of the grid site resources used by each experiment such as CMS)
    - Cluster utilization data
