import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from vera_testframework.pyspark import (
    ReferentiedataTest,  # Adjust the import according to your module structure
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("pytest").getOrCreate()


@pytest.fixture
def ruimten_df(spark):
    ruimten = [
        (1, "LOG", "Loggia"),
        (2, "WOO", "Woonkamer"),
        (3, "BAD", "Badruimte"),
        (4, "BAD", "Badkamer"),
        (5, None, "Kelder"),
        (6, "SLA", None),
    ]
    return spark.createDataFrame(ruimten, ["id", "code", "naam"])


def test_referentiedata_valid_code(ruimten_df):
    test = ReferentiedataTest(soort="RUIMTEDETAILSOORT", attribuut="Code")

    # Get valid codes from referentiedata_df in the test object
    valid_codes = set(
        row["Code"]
        for row in test.referentiedata_df.filter(col("Soort") == "RUIMTEDETAILSOORT")
        .select("Code")
        .collect()
    )

    # Apply the test
    result_df = test.test(ruimten_df, "code", "id", False)

    # Collect the results
    results = result_df.select("code", "code__VERAStandaard").collect()
    for row in results:
        if row["code"] is not None:
            assert (row["code"] in valid_codes) == row["code__VERAStandaard"]
        else:
            assert row["code__VERAStandaard"] is False


def test_referentiedata_valid_naam(ruimten_df):
    test = ReferentiedataTest(soort="RUIMTEDETAILSOORT", attribuut="Naam")

    # Get valid names from referentiedata_df in the test object
    valid_namen = set(
        row["Naam"]
        for row in test.referentiedata_df.filter(col("Soort") == "RUIMTEDETAILSOORT")
        .select("Naam")
        .collect()
    )

    # Apply the test
    result_df = test.test(ruimten_df, "naam", "id", False)

    # Collect the results
    results = result_df.select("naam", "naam__VERAStandaard").collect()
    for row in results:
        if row["naam"] is not None:
            assert (row["naam"] in valid_namen) == row["naam__VERAStandaard"]
        else:
            assert row["naam__VERAStandaard"] is False


def test_referentiedata_invalid_soort():
    with pytest.raises(ValueError):
        ReferentiedataTest(soort="INVALID", attribuut="Code")


def test_referentiedata_invalid_attribuut():
    with pytest.raises(ValueError):
        ReferentiedataTest(soort="RUIMTEDETAILSOORT", attribuut="InvalidAttribuut")


def test_wrong_type_soort():
    with pytest.raises(TypeError):
        ReferentiedataTest(soort=123, attribuut="Code")


def test_str_and_repr():
    assert (
        str(ReferentiedataTest(soort="RUIMTEDETAILSOORT", attribuut="Code"))
        == "ReferentiedataTest(RUIMTEDETAILSOORT, Code)"
    )
    assert (
        repr(ReferentiedataTest(soort="RUIMTEDETAILSOORT", attribuut="Code"))
        == "ReferentiedataTest(RUIMTEDETAILSOORT, Code)"
    )
