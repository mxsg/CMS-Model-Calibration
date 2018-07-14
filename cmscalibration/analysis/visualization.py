import utils.report as rp
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
