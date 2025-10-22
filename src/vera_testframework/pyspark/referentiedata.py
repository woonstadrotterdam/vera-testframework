from typing import Literal, Optional

from testframework.dataquality.tests import ValidCategory

from vera_testframework.utils import get_csv_from_release


class ReferentiedataTest(ValidCategory):  # type: ignore
    """
    Initialize a ReferentiedataTest instance.

    This test validates that column values match the VERA referentiedata standard.
    Results are returned in long-format with columns: primary_key, test_name, test_col,
    test_value, test_result, test_description.

    Args:
        name (Optional[str]): The name of the test. If not provided, defaults to "VERAStandaard".
        soort (str): The type/category of the data, which will be converted to uppercase.
        attribuut (Literal["Code", "Naam"]): The attribute to use, either "Code" or "Naam". It will be capitalized.
        release (str): The tag of the release to use. Default is "latest".

    Raises:
        TypeError: If soort is not a string.
        ValueError: If attribuut is not "Code" or "Naam".
    """

    _referentiedata_cache: dict[str, list[dict[str, str]]] = {}

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        soort: str,
        attribuut: Literal["Code", "Naam"],
        release: str = "latest",
    ):
        if not isinstance(soort, str):
            raise TypeError("soort must be a string")
        if attribuut not in ["Code", "Naam"]:
            raise ValueError("attribuut must be either 'Code' or 'Naam'")

        self.soort = soort.upper()
        self.attribuut = attribuut.capitalize()
        self.release = release.upper()
        name = name if name else f"VERA_{self.release}_{self.soort}_{self.attribuut}"

        self.referentiedata = self._get_cached_data(release)
        super().__init__(name=name, categories=self._categorieen())

    @classmethod
    def _get_cached_data(cls, release_tag: str) -> list[dict[str, str]]:
        if release_tag not in cls._referentiedata_cache:
            cls._referentiedata_cache[release_tag] = get_csv_from_release(
                release_tag=release_tag
            )
        return cls._referentiedata_cache[release_tag]

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

    def __str__(self) -> str:
        return f"ReferentiedataTest({self.soort}, {self.attribuut}, v={self.release})"

    def __repr__(self) -> str:
        return self.__str__()
