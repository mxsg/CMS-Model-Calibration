import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.colors import LogNorm

import utils.report as rp
from analysis import cpuefficiency, resource_usage
from data.dataset import Dataset, Metric


def jobtype_distribution(dataset: Dataset):
    job_df = dataset.df

    summary = job_df.groupby(Metric.JOB_TYPE.value).size()

    plt.figure()
    # Plot summary as a series
    axes = summary.plot.pie(title='Job Count per Type', autopct='%1.1f%%')

    axes.axis('equal')
    axes.set_ylabel('')
    axes.legend(title="Job Types")

    fig = axes.get_figure()

    return fig, axes
    # fig.savefig(path)


def jobtypes_over_time(dataset: Dataset, date_col=Metric.STOP_TIME.value, type_col=Metric.JOB_TYPE.value):
    df = dataset.df

    # df = df.set_index(Metric.STOP_TIME.value)
    # df = df.resample('1H', how='count')

    # Use crosstab to get pivot table with Job types in the columns
    df = pd.crosstab(index=[df[date_col]], columns=[df[type_col]])
    df = df.resample('1D').sum()

    # df2 = df.groupby(df[Metric.STOP_TIME.value].dt.hour).count()[Metric.JOB_TYPE.value]
    # pivot_df = df2.pivot(index=Metric.STOP_TIME.value, columns=Metric)

    # stacked =True
    plt.figure()
    axes = df.plot.bar(stacked=True)
    fig = axes.get_figure()

    return fig, axes
    # fig.savefig(path)


def jobtypes_over_time_df(df, date_col=Metric.STOP_TIME.value, type_col=Metric.JOB_TYPE.value):
    # Pivot table to get types as columns

    # date | type | count        date | a  | b
    # 1    | a    | 10      -->  1    | 10 | 20
    # 1    | b    | 20

    jobs_counts = df.pivot(index=date_col, columns=type_col, values='count').fillna(0)

    plt.figure()
    axes = jobs_counts.plot.bar(stacked=True)
    fig = axes.get_figure()

    return fig, axes


def demand_histogram(df, cutoff_quantile=0.98):
    if df.empty:
        return None, None

    x = df['CPUDemand']
    y = df['CPUIdleTime']

    x_max = x.quantile(cutoff_quantile)
    y_max = y.quantile(cutoff_quantile)

    fig, axes = plt.subplots()
    counts, xedges, yedges, im = axes.hist2d(x, y, bins=50, range=[[0, x_max], [0, y_max]], norm=LogNorm())

    # Add legend for the colors
    fig.colorbar(im)

    return fig, axes


def jobslot_usage(df, resample_freq='1H'):
    # Resample time series
    jobslot_usage = df['totalSlots'].resample(resample_freq).mean()

    # fig, axes = plt.subplots()
    plt.figure()
    axes = jobslot_usage.plot.line()
    fig = axes.get_figure()
    fig.tight_layout()

    return fig, axes


def multiple_jobslot_usage(series_dict, resample_freq='1H'):
    fig, axes = plt.subplots()

    for name, series in series_dict.items():
        # Resample time series
        usage_ts = series.resample(resample_freq).mean()
        usage_ts.plot.line(ax=axes, label=name)

    axes.set_xlabel('Time')
    axes.set_ylabel('Allocated job slots')
    axes.legend()

    fig.tight_layout()

    return fig, axes


def add_jobs_report_section(dataset: Dataset, report: rp.ReportBuilder):
    """Add a section including general job information to the markdown report."""

    report.append_paragraph("## Job dataset '{}'".format(dataset.name))
    report.append()

    # Count number of days
    day_count = (dataset.end - dataset.start).days

    report.append("**Start:** {}  \n**End:** {}  \n**Days:** {}".format(dataset.start,
                                                                        dataset.end,
                                                                        day_count))
    report.append()

    df = dataset.df.copy()

    # TODO This should be removed again later!
    report.append("Total job number: {}  ".format(df.shape[0]))
    report.append("Number of jobs without JobCategory: {}  ".format(df[Metric.JOB_CATEGORY.value].isnull().sum()))
    report.append("Number of jobs without JobType: {}  ".format(df[Metric.JOB_TYPE.value].isnull().sum()))

    report.append(
        "Number of jobs without core count information: {}  ".format(df[Metric.USED_CORES.value].isnull().sum()))

    # TODO Summarize all null values in data! (iterate through all columns and sets etc.)

    report.append("### Core count information")

    df_filled = df.copy()
    df_filled[Metric.USED_CORES.value] = df_filled[Metric.USED_CORES.value].fillna(-1)

    add_frame_to_report(df_filled[Metric.USED_CORES.value].value_counts(), report)

    core_counts_per_workflow = df_filled.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].nunique()

    # # Filter out all workflows where all core counts are the same
    # multiple_core_counts = df_filled.groupby(Metric.WORKFLOW.value).filter(
    #     lambda x: x[Metric.USED_CORES.value].nunique() != 1)

    # add_frame_to_report(core_counts_per_workflow, report)

    # report.append("Overview over ")
    # add_frame_to_report(
    #     multiple_core_counts.groupby([Metric.WORKFLOW.value, Metric.USED_CORES.value]).size().unstack(fill_value=0),
    #     report)

    report.append("### Job Category/Job Type Information")

    category_summary = df.groupby(Metric.JOB_CATEGORY.value).size().reset_index()

    code = rp.CodeBlock().append(category_summary.to_string())
    report.append_paragraph(code)

    # Fill job types by adding an unknown value
    df[Metric.JOB_TYPE.value] = df[Metric.JOB_TYPE.value].fillna('++unknown++')
    df[Metric.JOB_CATEGORY.value] = df[Metric.JOB_CATEGORY.value].fillna('++unknown++')

    category_summary = df.groupby([Metric.JOB_CATEGORY.value, Metric.JOB_TYPE.value]).size().reset_index()

    code = rp.CodeBlock().append(category_summary.to_string())
    report.append_paragraph(code)

    # Count number of jobs of each type
    summary = df.groupby(Metric.JOB_TYPE.value).size().reset_index()
    summary.columns = ['type', 'count']

    summary['countPerDay'] = summary['count'] / day_count
    summary['relFrequency'] = summary['count'] / summary['count'].sum()

    summary_code = rp.CodeBlock().append(summary.to_string())
    report.append_paragraph(summary_code)
    report.append()

    # Add information about job types per workflow
    # Todo Add this!
    # task_name_summary = df[Metric.TASK_NAME.value].fillna('++unknown++').value_counts()
    #
    # report.append("Different task names:  ")
    # report.append_paragraph(rp.CodeBlock().append(task_name_summary.to_string()))

    # Add figures of distribution
    fig, axes = jobtype_distribution(dataset)
    report.add_figure(fig, axes, 'jobtypes_pie')

    fig, axes = jobtypes_over_time(dataset)
    report.add_figure(fig, axes, 'jobtypes_histogram')

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

    report.append("### Total (CPU time/wall time) efficiency per job type")

    cpu_efficiencies = df.groupby(Metric.JOB_TYPE.value).apply(cpuefficiency.cpu_efficiency)
    add_frame_to_report(cpu_efficiencies, report)

    report.append("### Job Demands")

    job_types = df[Metric.JOB_TYPE.value].unique()

    job_type_groups = df.groupby(Metric.JOB_TYPE.value)

    for job_type, jobs in job_type_groups:
        report.append("CPU Demand and Idle Time for jobs of type {}".format(job_type))
        jobtype_df = jobs[(jobs['CPUDemand'].notnull()) & (jobs['CPUIdleTime'].notnull())]
        fig, axes = demand_histogram(jobtype_df)
        if fig is not None:
            report.add_figure(fig, axes, 'job_demands_{}'.format(job_type))

    report.append("#### Jobslot usage overview")
    report.append()

    mean_jobslots = resource_usage.mean_jobslot_usage(df, dataset.start, dataset.end,
                                                      start_ts_col=Metric.START_TIME.value,
                                                      end_ts_col=Metric.STOP_TIME.value,
                                                      slot_col=Metric.USED_CORES.value)

    jobslot_timeseries = resource_usage.calculate_jobslot_usage(df, dataset.start, dataset.end,
                                                                start_ts_col=Metric.START_TIME.value,
                                                                end_ts_col=Metric.STOP_TIME.value,
                                                                slot_col=Metric.USED_CORES.value)
    report.append("Mean number of jobslots used: {}  ".format(mean_jobslots))

    report.append("Jobslot usage over time:")

    fig, axes = jobslot_usage(jobslot_timeseries)
    report.add_figure(fig, axes, 'jobslot_usage')

    # report.append("## Job Event Counts")
    #
    # df = dataset.df.copy()
    #
    # # job_event_counts



def add_frame_to_report(df, report: rp.ReportBuilder):
    code = rp.CodeBlock().append(df.to_string())
    report.append_paragraph(code)
