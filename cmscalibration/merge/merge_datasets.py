import logging

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


class UnionDatasetMerge:

    def __init__(self, part_sep='#'):
        self.part_sep = part_sep

    def merge_datasets(self, matches, left: Dataset, right: Dataset, left_index, right_index, left_suffix='left',
                       right_suffix='right'):

        left_df = left.df
        right_df = right.df

        # Join with matches, preserving all entries in base dataframe
        # Right is base data frame, so use right merge
        half_joined = matches.join(left_df, on=left_index, how='right')

        # Preserve all entries, even those not in
        joined = half_joined.join(right_df, on=right_index, how='outer', lsuffix=left_suffix, rsuffix=right_suffix)

        # Reset index to the original index: ID of the left dataframe
        # Todo Create new identifier?
        # joined = joined.set_index(left_index)

        # Todo Actually join values

        # Columns that are suffixed are overlapping, included in both data sets
        for left_col in [col for col in joined.columns if col.endswith(left_suffix)]:
            col_name = self.remove_trailing(left_col, left_suffix)
            right_col = col_name + right_suffix

            # if right_col in right_df.columns:
            # If suffixed column exists, it also exists in other data set
            joined = self.merge_cols(joined, col_name, left_col, right_col)

        # for right_col in set(right_df.columns) - set(left_df.columns):
        #     # col_name = self.remove_trailing(right_col, right_suffix)
        #     joined[right_col] = joined[right_col + right_suffix]

        # Put result back into a dataset

        # Compute union of dates
        start_date = min(left.start, right.start)
        end_date = max(left.end, right.end)

        # Only retain extra data frames of the base dataset
        extra_dfs = left.extra_dfs

        result = Dataset(joined, left.name, start=start_date, end=end_date, sep=self.part_sep, extra_dfs=extra_dfs)
        return result

    def merge_cols(self, df, result_col, left_col, right_col):
        """Merge columns into a new column"""
        df[result_col] = df[left_col]
        df[result_col] = df[result_col].where(df[result_col].notnull(), df[right_col])
        logging.debug(
            "Column {}: left null: {}, right null: {}; result null: {}".format(result_col, df[left_col].isnull().sum(),
                                                                               df[right_col].isnull().sum(),
                                                                               df[result_col].isnull().sum()))

        return df

    @staticmethod
    def remove_trailing(full_string, trailing=''):
        trailing_len = len(trailing)
        if full_string[-trailing_len:] == trailing:
            return full_string[:-trailing_len]
        return full_string
