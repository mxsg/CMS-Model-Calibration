# Palladaio Import Model Data Format(s)



## Resource Environment

- One File per site (name is site name from manifest file)
- JSON-formatted

File format:

- top-level: list
- each list entry is a dictionary that describes a type of node
- properties:
    - cores: number of processor cores the machine has
    - cpu model: optional name for the node
    - hs06: HepSPEC06 score, linear benchmark for processing rate
    - interconnect: network speed
    - jobslots: integer that describes the number of cores that can be committed for jobs
    - count: number of identical nodes for the site


## Usage Model


## Open Questions

- all formats in one file?
- manifest file that has overview over separate files?
