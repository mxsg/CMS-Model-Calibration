# Architecture

The tool is designed to extract model calibration parameter descriptions from multiple data sets and heterogenous data sources.

It uses *Pandas* to handle data, as the datasets are large, but not too large to fit into memory and be efficiently analyzed as such. It allows to define custom workflows and invoke them by specifying them in the calibration configuration file.

## Notes

- Job monitoring information from CMS are available in two different formats from different subsystems, *JobMonitoring* and *WMArchive*. Data from these subsystems is overlapping, but not identical, and to acquire a comprehensive overview over the jobs that are run on the grid, these job reports have to be merged (they do not provide IDs or another simple way of merging them. Functionality to identify matches between them is located in `merge.reportmatching`.

- Workflows provide a starting point for analysis and can be configured in the calibration configuration file to be run as the tool is executed.



## Project Structure

Package structure:

- `workflows` contains the sequences used for a specific analysis, i.e. calibration of the GridKa site with local performance data, and CMS-provided job information.


- `data` contains a dataset abstraction (`dataset.Dataset`)
    - a dataset consists of a Pandas dataframe augmented with additional data (such as other associated data sets or metadata)
    - `Metric`s define the different types of information a dataset can include

- `interfaces` contains interfaces implemented as abstract class from the `ABC` package

- `importers` contains functionality to import data from local datasets.
    - Importers implemented as `MultiFileDataImporter` can be used to import certain subsets of datasets (based on dates) that are available as a group of files by using the `DatasetImporter` class
    - Importers define the structure the dataset they are designed to import is required to exhibit.
    - The `gridkadata` module contains classes to import data sets that are specific to the GridKa WLCG site, such as node performance information or the share of CMS-provided jobs at the site.
    - `wmaimport` and `jmimport` provide importers for the WMArchive and JobMonitoring job information described above.

- `merge` contains functionality related to matching multiple different datasets to each other:
    - `reportmatching` is used to identify matches between JobMonitoring and WMarchive job reports.
    - `job_node` matches job information to the performance information of the node it was executed on.

- `analysis` includes analysis functionality that operates on single data sets, such as the extraction of resource demands from matched job and node information (`demandextraction`).

- `exporters` contains export modules for the created calibration data (such as node information and job resource requirement information).


Additional, secondary project elements:

- `scripts` (repository root) includes
    - additional utility scripts
    - legacy data extraction scripts
- extraction of datasets from analytix is handled in scripts that extend extraction scripts provided by [the CMSSpark framework](https://github.com/vkuznet/cmsspark).
