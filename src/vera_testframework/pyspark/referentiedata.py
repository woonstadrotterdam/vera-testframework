from typing import Literal, Optional

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from testframework.dataquality.tests import ValidCategory


class ReferentiedataTest(ValidCategory):  # type: ignore
    """
    Initialize a ReferentiedataTest instance.

    Args:
        name (Optional[str]): The name of the test. If not provided, defaults to "VERAStandaard".
        soort (str): The type/category of the data, which will be converted to uppercase.
        attribuut (Literal["Code", "Naam"]): The attribute to use, either "Code" or "Naam". It will be capitalized.d
    """

    spark = SparkSession.builder.appName("vera_testframework").getOrCreate()
    referentiedata_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load("src/vera_testframework/data/Referentiedata.csv")
    )

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        soort: str,
        attribuut: Literal["Code", "Naam"],
    ):
        self.soort = soort
        self.attribuut = attribuut

        name = name if name else "VERAStandaard"
        super().__init__(name=name, categories=self._categorieen())

    def _categorieen(self) -> set[str]:
        categorieen_rows = (
            self.referentiedata_df.filter(F.col("Soort") == self.soort)
            .select(self.attribuut)
            .distinct()
            .collect()
        )
        if not categorieen_rows:
            mogelijke_soorten = [
                row["Soort"]
                for row in (self.referentiedata_df.select("Soort").distinct().collect())
            ]

            raise ValueError(
                f"Geen soorten gevonden voor soort '{self.soort}'. Opties zijn: {', '.join(sorted(mogelijke_soorten))}"
            )

        return {row[self.attribuut] for row in categorieen_rows}

    @property
    def soort(self) -> str:
        return self._soort

    @soort.setter
    def soort(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("soort must be a string")
        self._soort = value.upper()

    @property
    def attribuut(self) -> str:
        return self._attribuut

    @attribuut.setter
    def attribuut(self, value: Literal["Code", "Naam"]) -> None:
        if value not in ["Code", "Naam"]:
            raise ValueError("attribuut must be either 'Code' or 'Naam'")
        self._attribuut = value.capitalize()

    def __str__(self) -> str:
        return f"ReferentiedataTest({self.soort}, {self.attribuut})"

    def __repr__(self) -> str:
        return self.__str__()
