from typing import List

import pytest
from helpers.scd1 import scd1_merge
from pyspark.sql import Row, SparkSession
from pyspark.testing.utils import assertDataFrameEqual


@pytest.fixture
def spark_session():
    spark = SparkSession.builder.appName("Testing PySpark").getOrCreate()
    yield spark


def test_scd_1(spark_session: SparkSession):
    # testing basic scd 1 functionality : new row, updated row, null value handling
    df_target = spark_session.createDataFrame(
        data=[("1", "value 1"), ("2", "value 2")], schema=["code", "value"]
    )

    df_source = spark_session.createDataFrame(
        data=[("1", "value 1 updated"), ("3", "value 3"), ("4", None)],
        schema=["code", "value"],
    )

    actual_scd1_result_df = scd1_merge(
        df_source=df_source,
        df_target=df_target,
        primary_keys=["code"],
        manage_surrogate_key=False,
    )

    expected_scd1_result_df = spark_session.createDataFrame(
        data=[
            ("1", "value 1 updated"),
            ("2", "value 2"),
            ("3", "value 3"),
            ("4", None),
        ],
        schema=["code", "value"],
    )

    assertDataFrameEqual(actual_scd1_result_df, expected_scd1_result_df)


def test_scd_1_empty_target(spark_session: SparkSession):
    # The first time scd 1 is run, the target table is empty, so the result will be same as source
    df_target = None

    df_source = spark_session.createDataFrame(
        data=[("1", "value 1"), ("3", "value 3")], schema=["code", "value"]
    )

    actual_scd1_result_df = scd1_merge(
        df_source=df_source,
        df_target=df_target,
        primary_keys=["code"],
        manage_surrogate_key=False,
    )

    expected_scd1_result_df = df_source

    assertDataFrameEqual(actual_scd1_result_df, expected_scd1_result_df)


def test_scd_1_with_surrogate_key(spark_session: SparkSession):
    # testing surrogate key management
    df_source = spark_session.createDataFrame(
        data=[("3", "value 3"), ("4", "value 4")], schema=["code", "value"]
    )

    df_target = spark_session.createDataFrame(
        data=[
            ("1", "value 1", "d45c1856-ce5b-4d4f-9341-1f004d767537"),
            ("2", "value 2", "258a1c53-839b-4abb-88e7-493952ce8c17"),
        ],
        schema=["code", "value", "surrogate_key"],
    )

    actual_scd1_result_df = scd1_merge(
        df_source=df_source,
        df_target=df_target,
        primary_keys=["code"],
        manage_surrogate_key=True,
    )

    rows: List[Row] = actual_scd1_result_df.collect()

    assert len(rows) == 4

    for row in rows:
        if row["code"] == "1":
            assert row["value"] == "value 1"
            assert row["surrogate_key"] == "d45c1856-ce5b-4d4f-9341-1f004d767537"
        elif row["code"] == "2":
            assert row["value"] == "value 2"
            assert row["surrogate_key"] == "258a1c53-839b-4abb-88e7-493952ce8c17"
        elif row["code"] == "3":
            assert row["value"] == "value 3"
            assert row["surrogate_key"] is not None
            assert len(row["surrogate_key"]) == 36  # generated UUID length
        elif row["code"] == "4":
            assert row["value"] == "value 4"
            assert row["surrogate_key"] is not None
            assert len(row["surrogate_key"]) == 36


def test_scd_1_with_2_col_primary_key(spark_session: SparkSession):
    # testing basic scd 1 functionality : new row, updated row, null value handling
    df_target = spark_session.createDataFrame(
        data=[
            ("France", "Paris", "Farance, Paris"),
            ("Algeria", "Ain Sefra", "Algeria, Ain Sefra"),
        ],
        schema=["country", "city", "value"],
    )

    df_source = spark_session.createDataFrame(
        data=[
            ("France", "Paris", "Farance, Paris updated"),
            ("France", "Lyon", "France, Lyon"),
            ("Spain", "Seville", "Spain, Seville"),
            ("Portugal", "Lisbon", None),
        ],
        schema=["country", "city", "value"],
    )

    actual_scd1_result_df = scd1_merge(
        df_source=df_source,
        df_target=df_target,
        primary_keys=["country", "city"],
        manage_surrogate_key=False,
    )

    expected_scd1_result_df = spark_session.createDataFrame(
        data=[
            ("France", "Paris", "Farance, Paris updated"),
            ("Algeria", "Ain Sefra", "Algeria, Ain Sefra"),
            ("France", "Lyon", "France, Lyon"),
            ("Spain", "Seville", "Spain, Seville"),
            ("Portugal", "Lisbon", None),
        ],
        schema=["country", "city", "value"],
    )

    assertDataFrameEqual(actual_scd1_result_df, expected_scd1_result_df)
