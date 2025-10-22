# VERA Testframework

⏳ Work in progress

Maakt het makkelijk om te testen of data voldoet aan de [VERA-standaard](https://github.com/Aedes-datastandaarden/vera-referentiedata) m.b.v. het [pyspark-testframework](https://github.com/woonstadrotterdam/pyspark-testframework).

## Tutorial

**Op het moment is het _vera-testframework_ alleen compatibel met _pyspark_.**

```python
from vera_testframework.pyspark import ReferentiedataTest
from pyspark.sql import SparkSession
from testframework.dataquality import DataFrameTester
```

```python
spark = SparkSession.builder.appName("vera_testframework").getOrCreate()
```

**Hieronder wordt een voorbeeld DataFrame gemaakt m.b.t. ruimten, waarvan we gaan testen of de waardes voldoen aan de VERA-standaard.**

```python
ruimten = [
    (1, "LOG", "Loggia"),
    (2, "WOO", "Woonkamer"),
    (3, "BAD", "Badruimte"),
    (4, "BAD", "Badkamer"),
    (5, None, "Kelder"),
    (6, "SLA", None),
]

ruimten_df = spark.createDataFrame(ruimten, ["id", "code", "naam"])
```

**We maken gebruik van de `DataFrameTester` van het _pyspark-testframework_ om onze testresultaten in bij te houden.**

```python
testframework = DataFrameTester(
    df=ruimten_df,
    primary_key="id",
    spark=spark,
)
```

**Door middel van de `ReferentiedataTest` kunnen we testen of een kolom voldoet aan de VERA-standaard m.b.t. Referentiedata.**

```python
testframework.test(
    col="code",
    test=ReferentiedataTest(
        soort="RUIMTEDETAILSOORT",
        attribuut="Code",
        release="latest",  # standaard is latest, maar kan ook een specifieke versie zijn zoals v4.1.240419
    ),
    nullable=False,  # of een waarde leeg mag zijn. Dit is aan de gebruiker
).show(truncate=False)
```

    +-----------+----------------------------------------+-----------+----------+----------------------------------+--------+
    |primary_key|test_name                               |test_result|test_value|test_description                  |test_col|
    +-----------+----------------------------------------+-----------+----------+----------------------------------+--------+
    |1          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |LOG       |VERA_LATEST_RUIMTEDETAILSOORT_Code|code    |
    |2          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |WOO       |VERA_LATEST_RUIMTEDETAILSOORT_Code|code    |
    |3          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |BAD       |VERA_LATEST_RUIMTEDETAILSOORT_Code|code    |
    |4          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |BAD       |VERA_LATEST_RUIMTEDETAILSOORT_Code|code    |
    |5          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|false      |NULL      |VERA_LATEST_RUIMTEDETAILSOORT_Code|code    |
    |6          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |SLA       |VERA_LATEST_RUIMTEDETAILSOORT_Code|code    |
    +-----------+----------------------------------------+-----------+----------+----------------------------------+--------+

```python
testframework.test(
    col="naam",
    test=ReferentiedataTest(
        soort="RUIMTEDETAILSOORT",
        attribuut="Naam",
    ),
    nullable=True,
    description="Naam van ruimtedetailsoort voldoet aan de VERA-standaard"
).show(truncate=False)
```

    +-----------+----------------------------------------+-----------+----------+--------------------------------------------------------+--------+
    |primary_key|test_name                               |test_result|test_value|test_description                                        |test_col|
    +-----------+----------------------------------------+-----------+----------+--------------------------------------------------------+--------+
    |1          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Loggia    |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |
    |2          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Woonkamer |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |
    |3          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|false      |Badruimte |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |
    |4          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Badkamer  |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |
    |5          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Kelder    |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |
    |6          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |NULL      |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |
    +-----------+----------------------------------------+-----------+----------+--------------------------------------------------------+--------+

**De resultaten van de testen zijn te vinden in de `.results` attribuut van de `DataFrameTester`.**

```python
testframework.results.show(truncate=False)
```

    +-----------+----------------------------------------+-----------+----------+--------------------------------------------------------+--------+---------------+-----------------------+
    |primary_key|test_name                               |test_result|test_value|test_description                                        |test_col|primary_key_col|timestamp              |
    +-----------+----------------------------------------+-----------+----------+--------------------------------------------------------+--------+---------------+-----------------------+
    |1          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |LOG       |VERA_LATEST_RUIMTEDETAILSOORT_Code                      |code    |id             |2025-10-21 15:58:35.475|
    |2          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |WOO       |VERA_LATEST_RUIMTEDETAILSOORT_Code                      |code    |id             |2025-10-21 15:58:35.475|
    |3          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |BAD       |VERA_LATEST_RUIMTEDETAILSOORT_Code                      |code    |id             |2025-10-21 15:58:35.475|
    |4          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |BAD       |VERA_LATEST_RUIMTEDETAILSOORT_Code                      |code    |id             |2025-10-21 15:58:35.475|
    |5          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|false      |NULL      |VERA_LATEST_RUIMTEDETAILSOORT_Code                      |code    |id             |2025-10-21 15:58:35.475|
    |6          |code__VERA_LATEST_RUIMTEDETAILSOORT_Code|true       |SLA       |VERA_LATEST_RUIMTEDETAILSOORT_Code                      |code    |id             |2025-10-21 15:58:35.475|
    |1          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Loggia    |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |id             |2025-10-21 15:58:35.475|
    |2          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Woonkamer |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |id             |2025-10-21 15:58:35.475|
    |3          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|false      |Badruimte |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |id             |2025-10-21 15:58:35.475|
    |4          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Badkamer  |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |id             |2025-10-21 15:58:35.475|
    |5          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |Kelder    |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |id             |2025-10-21 15:58:35.475|
    |6          |naam__VERA_LATEST_RUIMTEDETAILSOORT_Naam|true       |NULL      |Naam van ruimtedetailsoort voldoet aan de VERA-standaard|naam    |id             |2025-10-21 15:58:35.475|
    +-----------+----------------------------------------+-----------+----------+--------------------------------------------------------+--------+---------------+-----------------------+

**Voor meer informatie over hoe het _pyspark-testframework_ te gebruiken, raadpleeg de documentatie op [hun Github](https://github.com/woonstadrotterdam/pyspark-testframework)**

```python
spark.stop()
```
