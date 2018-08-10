from data.dataset import Metric


def clean_job_reports(df):
    df = _add_missing_core_counts(df)
    return df


def _add_missing_core_counts(df):
    df_filled = df.copy()

    # TODO Only fill values where the result is unambiguous because only one non-null core count is present in the workflow.

    df_filled[Metric.USED_CORES.value] = df_filled.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].transform(
        lambda x: x.fillna(x.median()))

    # Core counts must be positive, so we can use negative value to fill misssing values
    # df_filled[Metric.USED_CORES.value] = df_filled[Metric.USED_CORES.value].fillna(-1)

    # Find workflows with only one unique non-null core count
    # Use unfilled data frame for this!

    # def fill_unique(group):
    #     if group[group > 0].nunique() == 1:
    #         unique_value = group[group > 0].unique()[0]
    #         group[group < 0] = unique_value
    #
    #     return group

    # single_core_count = df.groupby(Metric.WORKFLOW.value).filter(
    #     lambda x: x[Metric.USED_CORES.value].nunique() == 1)

    # df_filled[Metric.USED_CORES.value] = df_filled.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].apply(
    #     fill_unique)

    # Fill values in filtered data frame
    # df[Metric.USED_CORES.value] = single_core_count.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].apply(
    #     lambda x: x.ffill().bfill())

    # core_counts_per_workflow = df_filled.groupby(Metric.WORKFLOW.value)[Metric.USED_CORES.value].nunique()

    # Reset filled value again
    # df_filled.loc[df_filled[Metric.USED_CORES.value] < 0, Metric.USED_CORES.value] = np.NaN

    return df_filled
