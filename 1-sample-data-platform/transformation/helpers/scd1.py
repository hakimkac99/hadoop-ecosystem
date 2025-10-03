from functools import reduce

from pyspark.sql.functions import col

# TO DO : check duplicates


# Helpers to build conditions
def all_null(df_alias, columns):
    return reduce(
        lambda a, b: a & b, [col(f"{df_alias}.{c}").isNull() for c in columns]
    )


def any_not_null(df_alias, columns):
    return reduce(
        lambda a, b: a | b, [col(f"{df_alias}.{c}").isNotNull() for c in columns]
    )


# Helpers for column selection
def select_columns(df_alias, columns):
    return [col(f"{df_alias}.{c}") for c in columns]


def scd1_merge(df_source, df_target, primary_keys):
    """
    Implements Slowly Changing Dimension Type 1 (SCD1).

    Args:
        df_source: New data (source DataFrame)
        df_target: Existing data (target DataFrame)
        primary_keys: List of primary key column names

    Returns:
        DataFrame with SCD1 applied
    """

    # All non-key columns
    non_key_columns = [c for c in df_target.columns if c not in primary_keys]

    # Join source and target
    join_result = df_source.alias("source").join(
        df_target.alias("target"), on=primary_keys, how="full"
    )

    # Conditions
    source_null_cond = all_null("source", non_key_columns)
    target_null_cond = all_null("target", non_key_columns)
    source_notnull_cond = any_not_null("source", non_key_columns)
    target_notnull_cond = any_not_null("target", non_key_columns)

    # New rows (in source only)
    new_rows = join_result.where(source_notnull_cond & target_null_cond).select(
        *primary_keys, *select_columns("source", non_key_columns)
    )

    # Updated rows (present in both → overwrite with source)
    updated_rows = join_result.where(source_notnull_cond & target_notnull_cond).select(
        *primary_keys, *select_columns("source", non_key_columns)
    )

    # Rows to keep (only in target)
    rows_to_keep = join_result.where(source_null_cond & target_notnull_cond).select(
        *primary_keys, *select_columns("target", non_key_columns)
    )

    # Null rows (non key columns all null)
    null_rows = join_result.where(source_null_cond & target_null_cond).select(
        *primary_keys, *select_columns("target", non_key_columns)
    )

    # Final result
    scd1_result = rows_to_keep.union(updated_rows).union(new_rows).union(null_rows)

    return scd1_result
