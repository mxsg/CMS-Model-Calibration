# Job Monitoring Data Set Description

Analysis Notes:

- `TaskId`, `TaskMonitorId` both contain fewer distinct entries than `JobId`, number of entries: they refer to pilot job wrapper of jobs
- no column on its own is distinct for all entries
- `JobId` contains multiples (ratio about 3:1 entries:distincts values
- `FileName`s are not unique for every entry, but most `FileName`s only appear once
- `JobMonitorId` is `unknown` for all examined entries
- input data records do not contain any full duplicates across all columns
- most `JobExecExitTimeStamp` values are identical to the respective `FinishedTimeStamp`

Assumptions:

- All time stamps in the data refer to unix epoch data with base unit milliseconds
