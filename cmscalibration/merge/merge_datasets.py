from data.dataset import Dataset


class AugmentDatasetMerge:
    """This class implements a merging strategy on two datasets that is based on using values from one dataset
    to augment the ones from the other, but not including more entries"""

    def __init__(self, augment_sep='#', augment_suffix='augment'):
        self.augment_sep = augment_sep
        self.augment_suffix = augment_suffix

    def merge_datasets(self, matches, base: Dataset, augment: Dataset, left_index, right_index):
        """Merge two datasets based on a dataframe containing matches between their indices."""

        base_df = base.df
        augment_df = augment.df

        # Join with matches, preserving all entries in base dataframe
        half_joined = matches.join(base_df, on=left_index, how='right')

        joined = half_joined.join(augment_df, on=right_index, how='left', rsuffix=self.augment_suffix)

        # Reset index to the original index: ID of the base dataframe
        joined = joined.set_index(left_index)

        # Augment column values
        for base_column in base_df.columns:
            aug_col = base_column + self.augment_suffix

            if aug_col in joined.columns:
                joined = self.augment_column(joined, base_column, aug_col)

        # Put result back into a dataset

        # Compute union of dates
        start_date = min(base.start, augment.start)
        end_date = max(base.end, augment.end)

        # TODO: Retain extra data frames from both datasets?
        # Only retain extra data frames of the base dataset
        extra_dfs = base.extra_dfs

        result = Dataset(joined, base.name, start=start_date, end=end_date, sep=self.augment_sep, extra_dfs=extra_dfs)
        return result

    def augment_column(self, df, base_col, augment_col):
        """Fill empty values in a base column with values from the augmenting column."""

        df.loc[df[base_col].isnull(), base_col] = df[augment_col]
        return df
