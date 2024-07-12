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
).show()
```

    +---+----+-------------------+
    | id|code|code__VERAStandaard|
    +---+----+-------------------+
    |  1| LOG|               true|
    |  2| WOO|               true|
    |  3| BAD|               true|
    |  4| BAD|               true|
    |  5|NULL|              false|
    |  6| SLA|               true|
    +---+----+-------------------+

```python
testframework.test(
    col="naam",
    test=ReferentiedataTest(
        soort="RUIMTEDETAILSOORT",
        attribuut="Naam",
    ),
    nullable=True,
).show()
```

    +---+---------+-------------------+
    | id|     naam|naam__VERAStandaard|
    +---+---------+-------------------+
    |  1|   Loggia|               true|
    |  2|Woonkamer|               true|
    |  3|Badruimte|              false|
    |  4| Badkamer|               true|
    |  5|   Kelder|               true|
    |  6|     NULL|               true|
    +---+---------+-------------------+

**De resultaten van de testen zijn te vinden in de `.results` attribuut van de `DataFrameTester`.**

```python
testframework.results.show()
```

    +---+-------------------+-------------------+
    | id|code__VERAStandaard|naam__VERAStandaard|
    +---+-------------------+-------------------+
    |  1|               true|               true|
    |  2|               true|               true|
    |  3|               true|              false|
    |  4|               true|               true|
    |  5|              false|               true|
    |  6|               true|               true|
    +---+-------------------+-------------------+

**Voor meer informatie over hoe het _pyspark-testframework_ te gebruiken, raadpleeg de documentatie op [hun Github](https://github.com/woonstadrotterdam/pyspark-testframework)**
