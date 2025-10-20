import pytest
from pyspark.sql import SparkSession

# Adjust the import according to your module structure
from vera_testframework.pyspark import ReferentiedataTest


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

    # Get valid codes from referentiedata in the test object
    valid_codes = set(
        row["Code"]
        for row in test.referentiedata
        if row["Soort"] == "RUIMTEDETAILSOORT"
    )

    # Apply the test
    result_df = test.test(ruimten_df, "code", "id", False)

    # Expect canonical long-format columns
    expected_columns = {
        "primary_key",
        "test_name",
        "test_col",
        "test_value",
        "test_result",
        "test_description",
    }
    assert expected_columns.issubset(set(result_df.columns))

    # Collect the results and validate against ground truth
    results = result_df.collect()
    rows_by_pk = {row["primary_key"]: row for row in results}

    # Get original data for comparison
    original_data = ruimten_df.select("id", "code").collect()
    for row in original_data:
        pk = row["id"]  # Keep as integer to match the result format
        code = row["code"]
        expected_ok = (code in valid_codes) if code is not None else False

        assert pk in rows_by_pk, f"Primary key {pk} not found in results"
        result_row = rows_by_pk[pk]

        assert result_row["test_result"] == expected_ok
        assert result_row["test_name"] == "code__VERA_RUIMTEDETAILSOORT_Code"
        assert result_row["test_col"] == "code"
        # Handle None values correctly - Spark returns None for null values
        expected_value = str(code) if code is not None else None
        assert result_row["test_value"] == expected_value


def test_referentiedata_valid_naam(ruimten_df):
    test = ReferentiedataTest(soort="RUIMTEDETAILSOORT", attribuut="Naam")

    # Get valid names from referentiedata in the test object
    valid_namen = set(
        row["Naam"]
        for row in test.referentiedata
        if row["Soort"] == "RUIMTEDETAILSOORT"
    )

    # Apply the test
    result_df = test.test(ruimten_df, "naam", "id", False)

    # Expect canonical long-format columns
    expected_columns = {
        "primary_key",
        "test_name",
        "test_col",
        "test_value",
        "test_result",
        "test_description",
    }
    assert expected_columns.issubset(set(result_df.columns))

    # Collect the results and validate against ground truth
    results = result_df.collect()
    rows_by_pk = {row["primary_key"]: row for row in results}

    # Get original data for comparison
    original_data = ruimten_df.select("id", "naam").collect()
    for row in original_data:
        pk = row["id"]  # Keep as integer to match the result format
        naam = row["naam"]
        expected_ok = (naam in valid_namen) if naam is not None else False

        assert pk in rows_by_pk, f"Primary key {pk} not found in results"
        result_row = rows_by_pk[pk]

        assert result_row["test_result"] == expected_ok
        assert result_row["test_name"] == "naam__VERA_RUIMTEDETAILSOORT_Naam"
        assert result_row["test_col"] == "naam"
        # Handle None values correctly - Spark returns None for null values
        expected_value = str(naam) if naam is not None else None
        assert result_row["test_value"] == expected_value


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
        == "ReferentiedataTest(RUIMTEDETAILSOORT, Code, v=latest)"
    )
    assert (
        repr(ReferentiedataTest(soort="RUIMTEDETAILSOORT", attribuut="Code"))
        == "ReferentiedataTest(RUIMTEDETAILSOORT, Code, v=latest)"
    )


def test_wrong_release_tag():
    with pytest.raises(Exception):
        ReferentiedataTest(
            soort="RUIMTEDETAILSOORT", attribuut="Code", release="invalid"
        )
