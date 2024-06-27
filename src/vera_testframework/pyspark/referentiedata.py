from typing import Literal, Optional

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from testframework.dataquality.tests import ValidCategory


class ReferentiedataTest(ValidCategory):  # type: ignore
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
        self.soort = soort.upper()
        self.attribuut = attribuut.capitalize()
        categorieen_rows = (
            self.referentiedata_df.filter(F.col("Soort") == self.soort)
            .select(self.attribuut)
            .collect()
        )
        categorieen_set = {row[attribuut] for row in categorieen_rows}
        name = name if name else "VERAStandaard"
        super().__init__(name=name, categories=categorieen_set)

    def __str__(self) -> str:
        return f"ReferentiedataTest({self.soort}, {self.attribuut})"

    def __repr__(self) -> str:
        return self.__str__()
