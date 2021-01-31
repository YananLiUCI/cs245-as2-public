import org.apache.spark
//
// CS 245 Assignment 2, Part I starter code
//
// 1. Loads Cities and Countries as dataframes and creates views
//    so that we can issue SQL queries on them.
// 2. Runs 2 example queries, shows their results, and explains
//    their query plans.
//

// note that we use the default `spark` session provided by spark-shell
val cities = (spark.read
        .format("csv")
        .option("header", "true") // first line in file has headers
        .load("./Cities.csv"));
cities.createOrReplaceTempView("Cities")

val countries = (spark.read
        .format("csv")
        .option("header", "true")
        .load("./Countries.csv"));
countries.createOrReplaceTempView("Countries")

// look at the schemas for Cities and Countries
cities.printSchema()
countries.printSchema()

// Example 1
var df = spark.sql("SELECT city FROM Cities")
df.show()  // display the results of the SQL query
df.explain(true)  // explain the query plan in detail:
                  // parsed, analyzed, optimized, and physical plans

// Example 2
df = spark.sql("""
    SELECT *
    FROM Cities
    WHERE temp < 5 OR true
""")
df.show()
df.explain(true)

// P1
df = spark.sql("""
    SELECT country, EU
FROM Countries
WHERE coastline = "yes"
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(coastline="yes")(Countries)
//Optimized Logical plan
//σ(coastline="yes")(Countries)
// Explanation
//The analyzed and optimized plans are the exact same because there is no logical optimization for projecting a single column from a table

// P2
df = spark.sql("""
    SELECT city
FROM (
    SELECT city, temp
FROM Cities )
WHERE temp < 4
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(cast(temp as int) < 4)(Cities)
//Optimized Logical plan
//σ(cast(temp as int) < 4)(Cities)
// Explanation
//The analyzed and optimized plans are the exact same because there is no logical optimization for projecting a single column from a table

// P3
df = spark.sql("""
    SELECT *
FROM Cities, Countries
WHERE Cities.country = Countries.country
    AND Cities.temp < 4
    AND Countries.pop > 6
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(Cities.country = Countries.country && (cast(temp#14 as int) < 4)) && (cast(pop#31 as int) > 6)))
//Optimized Logical plan
//σ((cast(temp#14 as int) < 4)) && (cast(pop#31 as int) > 6)(σ(Cities.country = Countries.country))
// Explanation
//σ(Cities.country = Countries.country && (cast(temp#14 as int) < 4)) && (cast(pop#31 as int) > 6)))
//=σ((cast(temp#14 as int) < 4)) && (cast(pop#31 as int) > 6)(σ(Cities.country = Countries.country))

//P4
df = spark.sql("""
SELECT city, pop
FROM Cities, Countries
WHERE Cities.country = Countries.country
    AND Countries.pop > 6
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(Cities.country = Countries.country && (cast(Countries.pop as int) > 6)))
//Optimized Logical plan
//σ((cast(Countries.pop as int) > 6)(σ(Cities.country = Countries.country))
// Explanation
//σ(Cities.country = Countries.country && (cast(Countries.pop as int) > 6)))
//=σ((cast(Countries.pop as int) > 6)(σ(Cities.country = Countries.country))

//P5
df = spark.sql("""
SELECT *
FROM Countries
WHERE country LIKE "%e%d"
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(country like "%e%d")
//Optimized Logical plan
//σ(country like "%e%d")
// Explanation
//The analyzed and optimized plans are the exact same because there is no logical optimization for projecting a single column from a table

//P6
df = spark.sql("""
SELECT *
FROM Countries
WHERE country LIKE "%ia"
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(country like "%ia")
//Optimized Logical plan
//σ(country endsWith "%ia")
// Explanation
//σ(country like "%ia") = σ(country endsWith "%ia") As the ia will only occurs at the end of country name.

// P7
df = spark.sql("""
SELECT t1 + 1 as t2
FROM (
    SELECT cast(temp as int) + 1 as t1
FROM Cities )
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(σ(cast(temp as int) + 1) + 1)
//Optimized Logical plan
//σ(cast(temp as int) + 2)
// Explanation
//σ(σ(cast(temp as int) + 1) + 1) = σ(cast(temp as int) + 2)

//P8
df = spark.sql("""
SELECT t1 + 1 as t2
FROM (
    SELECT temp + 1 as t1
FROM Cities )
""")
df.show()
df.explain(true)
// Analyzed logical plan
//σ(σ(cast(temp as double) + cast(1 as double)) + cast(1 as double))
//Optimized Logical plan
//σ((cast(temp as int) + 1.0) + 1.0)
// Explanation
//σ(σ(cast(temp as double) + cast(1 as double)) + cast(1 as double))
//=σ((cast(temp as int) + 1.0) + 1.0)