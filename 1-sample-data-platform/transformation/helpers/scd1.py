from functools import reduce
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col


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


def scd1_merge(
    df_source: DataFrame,
    df_target: DataFrame,
    primary_keys: List[str],
    manage_surrogate_key: Optional[bool] = True,
    surrogate_key_column_name: Optional[str] = "surrogate_key",
):
    """
    Implements Slowly Changing Dimension Type 1 (SCD1).
    Source dataframe deduplication should be managed before calling scd1_merge()

    Args:
        df_source: New data (source DataFrame)
        df_target: Existing data (target DataFrame)
        primary_keys: List of primary key column names
        manage_surrogate_key: whether to add a surrogate key to the dimension
        surrogate_key_column_name: "surrogate_key" by default

    Returns:
        DataFrame with SCD1 applied
    """
    # Add surrogate key
    if manage_surrogate_key and surrogate_key_column_name:
        df_source = df_source.withColumn(surrogate_key_column_name, F.expr("uuid()"))

    # First merge
    if not df_target:
        return df_source

    # All non-key columns with excluding surrogate key column if existing
    non_key_columns = [
        c
        for c in df_target.columns
        if c not in primary_keys
        and not (manage_surrogate_key and c == surrogate_key_column_name)
    ]

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
        *primary_keys,
        *select_columns(
            "source",
            non_key_columns
            + ([surrogate_key_column_name] if manage_surrogate_key else []),
        ),
    )

    # Updated rows (present in both → overwrite with source), keep target's surrogate key if present
    updated_rows = join_result.where(source_notnull_cond & target_notnull_cond).select(
        *primary_keys,
        *select_columns("source", non_key_columns),
        *select_columns(
            "target", [surrogate_key_column_name] if manage_surrogate_key else []
        ),
    )

    # Rows to keep (only in target)
    rows_to_keep = join_result.where(source_null_cond & target_notnull_cond).select(
        *primary_keys,
        *select_columns(
            "target",
            non_key_columns
            + ([surrogate_key_column_name] if manage_surrogate_key else []),
        ),
    )

    # Null rows (non key columns all null)
    null_rows = join_result.where(source_null_cond & target_null_cond).select(
        *primary_keys,
        *select_columns(
            "target",
            non_key_columns
            + ([surrogate_key_column_name] if manage_surrogate_key else []),
        ),
    )

    # Final result
    scd1_result = rows_to_keep.union(updated_rows).union(new_rows).union(null_rows)

    return scd1_result
