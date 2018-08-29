# CMS Performance Model Calibration

This project provides software for the automated analysis of CMS jobs running on the GridKa WLCG site with the goal of computing model calibration parameters for Palladio performance models of the grid.

## Installation and Requirements

This project requires a Python installation with the following requirements:

- Python 3.6+ recommended
- Pandas (install via `pip install pandas`)
- Numpy (install via `pip install numpy`)

A CERN computing account and access to the *analytix* cluster is required for the extraction of monitoring information from CERN directly. The software can be used to compute calibration parameters from locally available datasets without the need for such access.

## Usage

To run the software, follow these steps:

1. To use this tool, a calibration run configuration file is required.
    - This configuration file contains metadata about the analysis to be run, dataset locations and output information.
    - More details can be found [below](#input-files).
    - The file also contains the location of the correct calibration workflow to be run upon executing the tool.
    - The following assumes a valid configuration file at path `<path/calibration-conf.json>`

2. Place your datasets in the locations indicated in the calibration configuration file.

3. Run the tool by invoking `python cmscalibration --conf <path/calibration-conf.json>`.

    If no path to a configuration file is given, the path is assumed to be `calibration.json` in the working directory the tool is run from.


## [Input Files](#input-files)

More information about the input files required to run the software can be found on [a separate page](docs/input-files.md).

## Architecture Overview

An overview over the architecture of the tool can be found [in a separate document](docs/architecture-overview.md).
