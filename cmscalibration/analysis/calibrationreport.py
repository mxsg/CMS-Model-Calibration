import utils.report as rp
from analysis import cpuefficiency
from data.dataset import Dataset, Metric


def add_jobs_report_section(dataset: Dataset, report: rp.ReportBuilder):
    report.append_paragraph("## Job dataset '{}'".format(dataset.name))
    report.append()

    # Count number of days
    day_count = (dataset.end - dataset.start).days

    report.append("**Start:** {}  \n**End:** {}  \n**Days:** {}".format(dataset.start,
                                                                        dataset.end,
                                                                        day_count))
    report.append()

    df = dataset.df

    # Count number of jobs of each type
    summary = df.groupby(Metric.JOB_TYPE.value).size().reset_index()
    summary.columns = ['type', 'count']

    summary['countPerDay'] = summary['count'] / day_count
    summary['relFrequency'] = summary['count'] / summary['count'].sum()

    summary_code = rp.CodeBlock().append(summary.to_string())
    report.append_paragraph(summary_code)
    report.append()

    # Add general metadata
    report.append("Total jobs: {}  ".format(df.shape[0]))
    report.append("Total days: {}  ".format(day_count))
    report.append("Jobs per day: {}  ".format(summary['countPerDay'].sum()))

    report.append()
    report.append("**Job efficiencies**:  ")

    cpu_eff = cpuefficiency.cpu_efficiency(df)
    report.append("Total (CPU time/wall time) efficiency: {}  ".format(cpu_eff))

    cpu_eff = cpuefficiency.cpu_efficiency_scaled_by_jobslots(df)
    report.append("Total (CPU time/wall time) efficiency scaled (jobslot + virtual cores): {}  ".format(cpu_eff))

    cpu_eff = cpuefficiency.cpu_efficiency_scaled_by_jobslots(df, physical=True)
    report.append("Total (CPU time/wall time) efficiency scaled (jobslot + physical cores): {}  ".format(cpu_eff))
