import csv
from typing import Literal, Optional

from testframework.dataquality.tests import ValidCategory


class ReferentiedataTest(ValidCategory):  # type: ignore
    """
    Initialize a ReferentiedataTest instance.

    Args:
        name (Optional[str]): The name of the test. If not provided, defaults to "VERAStandaard".
        soort (str): The type/category of the data, which will be converted to uppercase.
        attribuut (Literal["Code", "Naam"]): The attribute to use, either "Code" or "Naam". It will be capitalized.
    """

    with open(
        "src/vera_testframework/data/Referentiedata.csv", newline="", encoding="utf-8"
    ) as csvfile:
        referentiedata = [row for row in csv.DictReader(csvfile, delimiter=";")]

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
        categorieen_rows = [
            row for row in self.referentiedata if row["Soort"] == self.soort
        ]
        if not categorieen_rows:
            mogelijke_soorten = {row["Soort"] for row in self.referentiedata}
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
