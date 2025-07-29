# Explore Hudi Table & Query Types

In this section, you will create different types of Hudi tables with Spark and query them with Presto.

This section is comprised of the following steps:

- [Explore Hudi Table \& Query Types](#explore-hudi-table--query-types)
  - [1. Create MoR Hudi table](#1-create-mor-hudi-table)
  - [2. Query MoR table with Presto](#2-query-mor-table-with-presto)
  - [3. Add data to MoR table and query](#3-add-data-to-mor-table-and-query)
  - [4. Create CoW Hudi table](#4-create-cow-hudi-table)
  - [5. Query CoW table with Presto](#5-query-cow-table-with-presto)

## 1. Create MoR Hudi table

In this section we'll explore Hudi Merge-on-Read (MoR) tables. MoR tables store data using file versions with combination of columnar (e.g parquet) + row based (e.g avro) file formats. Updates are logged to delta files & later compacted to produce new versions of columnar files synchronously or asynchronously. Currently, it is not possible to create Hudi tables from Presto, so we will use Spark to create our tables. To do so, we'll enter the Spark container and start the `spark-shell`:

```sh
docker exec -it spark /opt/spark/bin/spark-shell
```

It may take a few moments to initialize before you see the `>scala` prompt, indicating that the shell is ready to accept commands. Enter "paste" mode by typing the following and pressing enter:

```sh
>scala :paste

// Entering paste mode (ctrl-D to finish)
```

Copy and paste the below code, which imports required packages, creates a Spark session, and defines some variables that we will reference in subsequent code.

```scala
import org.apache.spark.sql.{SparkSession, SaveMode}
import scala.util.Random
import java.util.UUID

val spark = SparkSession.builder()
  .appName("HudiToMinIO")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.sql.catalogImplementation", "hive")
  .config("hive.metastore.uris", "thrift://hive-metastore:9083")
  .config("spark.sql.hive.convertMetastoreParquet", "false")
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
  .config("spark.hadoop.fs.s3a.access.key", "minio")
  .config("spark.hadoop.fs.s3a.secret.key", "minio123")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val basePath = "s3a://warehouse/hudi-tables"
val dbName = "default"

def hudiOptions(tableName: String, tableType: String, precombineField: String, partitionField: Option[String] = None): Map[String, String] = {
  Map(
    "hoodie.table.name" -> tableName,
    "hoodie.datasource.write.recordkey.field" -> "uuid",
    "hoodie.datasource.write.precombine.field" -> precombineField,
    "hoodie.datasource.write.table.name" -> tableName,
    "hoodie.datasource.write.operation" -> "upsert",
    "hoodie.datasource.write.table.type" -> tableType,
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
    "hoodie.datasource.hive_sync.enable" -> "true",
    "hoodie.datasource.hive_sync.mode" -> "hms",
    "hoodie.datasource.hive_sync.database" -> dbName,
    "hoodie.datasource.hive_sync.table" -> tableName,
  ) ++ partitionField.map(f => Map("hoodie.datasource.write.partitionpath.field" -> f)).getOrElse(Map.empty)
}

```

Make sure you include a newline character at the very end. Press `Ctrl+D` to begin executing the pasted code.

We will complete the same process with our next code block, which will create and populate our MoR table with randomly generated data about taxi trips. Notice that we are including an extra column, `commit_num` that will show us the commit in which any given row was added.

```scala
val dataGen = new DataGenerator
val inserts = convertToStringList(dataGen.generateInserts(10))
val data = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

val morTableName = "mor_trips_table"

data.withColumn("commit_num", lit("update1")).write.format("hudi").
    options(getQuickstartWriteConfigs).
    options(hudiOptions(morTableName, "MERGE_ON_READ", "ts")).
    mode(Overwrite).
    save(s"$basePath/$morTableName");
```

Before we go on to query these tables, let's take a look at what files and directories have been created for this table in our s3 storage. Go to MinIO UI (`localhost:9091`) and log in with the username and password that we defined in `docker-compose.yaml` (`minio`/`minio123`). Under the `hudi-tables` path, there should be a single sub-path called `mor_trips_table`. Click into this path and explore the created files and directory structure, especially those in the `.hoodie` directory. This is where `.hoodie` keeps metadata for the `mor_trips_table`. We can see that there is one set of `deltacommit` files created to keep track of the initial data we've inserted into the table.

![MoR metadata directory](../images/mor_dirs.png)

## 2. Query MoR table with Presto

Now let's query these tables with Presto. In a new terminal tab or window, exec into the Presto container and start the Presto CLI to query our table.

```sh
 docker exec -it coordinator presto-cli
```

We first specify that we want to use the Hudi catalog and `default` schema for all queries here on out. Then, execute a `show tables` command:

```sh
presto> use hudi.default;
USE
presto:default> show tables;
       Table        
--------------------
 mor_trips_table_ro 
 mor_trips_table_rt 
(2 rows)
```

Notice how Hudi has implicity created two versions of the MoR table - one suffixed with `_ro` for "read-optimized" and one suffixed with `_rt` for "real-time". As expected, each provides a different view. Right now, querying them shows the same information since we've only inserted data into the table once at time of creation. Run the below query on both tables to verify this.

```sh
presto:default> select commit_num, fare, begin_lon, begin_lat, ts from mor_trips_table_rt;
 commit_num |        fare        |      begin_lon      |      begin_lat      |      ts       
------------+--------------------+---------------------+---------------------+---------------
 update1    |  93.56018115236618 | 0.14285051259466197 | 0.21624150367601136 | 1753612150659 
 update1    |  27.79478688582596 |  0.6273212202489661 | 0.11488393157088261 | 1753220168185 
 update1    |  66.62084366450246 | 0.03844104444445928 |  0.0750588760043035 | 1753592423892 
 update1    | 34.158284716382845 | 0.46157858450465483 |  0.4726905879569653 | 1753164447671 
 update1    |  64.27696295884016 |  0.4923479652912024 |  0.5731835407930634 | 1753509890278 
 update1    |  41.06290929046368 |  0.8192868687714224 |   0.651058505660742 | 1753209120536 
 update1    | 17.851135255091155 |  0.5644092139040959 |    0.40613510977307 | 1753058046638 
 update1    |  33.92216483948643 |  0.9694586417848392 |  0.1856488085068272 | 1753301302307 
 update1    | 19.179139106643607 |  0.7528268153249502 |  0.8742041526408587 | 1753125594865 
 update1    |   43.4923811219014 |  0.8779402295427752 |  0.6100070562136587 | 1753163044342 
(10 rows)
```

## 3. Add data to MoR table and query

Now, let's go back to our `spark-shell` terminal tab and add more data to our tables using paste mode. Note that our `commit_num` column value has changed.

```
val updates = convertToStringList(dataGen.generateUpdates(10))
val updatedData = spark.read.json(spark.sparkContext.parallelize(updates, 2));

updatedData.withColumn("commit_num", lit("update2")).write.format("hudi").
    options(getQuickstartWriteConfigs).
    options(hudiOptions(morTableName, "MERGE_ON_READ", "ts")).
    mode(Append).
    save(s"$basePath/$morTableName");
```

Now if we query the tables in the Presto CLI, we see that the MoR `RO` ("read-optimized") and `RT` ("real-time") tables are starting to look different. As you can guess by the names, the RT table has the freshest data, and the RO table still shows our previous state.

```sh
presto:default> select commit_num, fare, begin_lon, begin_lat, ts from mor_trips_table_ro;
 commit_num |        fare        |      begin_lon      |      begin_lat      |      ts       
------------+--------------------+---------------------+---------------------+---------------
 update1    |  93.56018115236618 | 0.14285051259466197 | 0.21624150367601136 | 1753612150659 
 update1    |  27.79478688582596 |  0.6273212202489661 | 0.11488393157088261 | 1753220168185 
 update1    |  66.62084366450246 | 0.03844104444445928 |  0.0750588760043035 | 1753592423892 
 update1    | 34.158284716382845 | 0.46157858450465483 |  0.4726905879569653 | 1753164447671 
 update1    |  64.27696295884016 |  0.4923479652912024 |  0.5731835407930634 | 1753509890278 
 update1    |  41.06290929046368 |  0.8192868687714224 |   0.651058505660742 | 1753209120536 
 update1    | 17.851135255091155 |  0.5644092139040959 |    0.40613510977307 | 1753058046638 
 update1    |  33.92216483948643 |  0.9694586417848392 |  0.1856488085068272 | 1753301302307 
 update1    | 19.179139106643607 |  0.7528268153249502 |  0.8742041526408587 | 1753125594865 
 update1    |   43.4923811219014 |  0.8779402295427752 |  0.6100070562136587 | 1753163044342 
(10 rows)

Query 20250727_223116_00003_h4y8p, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
[Latency: client-side: 341ms, server-side: 317ms] [10 rows, 426KB] [31 rows/s, 1.31MB/s]

presto:default> select commit_num, fare, begin_lon, begin_lat, ts from mor_trips_table_rt;
 commit_num |        fare        |      begin_lon       |      begin_lat      |      ts       
------------+--------------------+----------------------+---------------------+---------------
 update1    |  93.56018115236618 |  0.14285051259466197 | 0.21624150367601136 | 1753612150659 
 update2    |   98.3428192817987 |   0.3349917833248327 |  0.4777395067707303 | 1753070771758 
 update2    |  63.72504913279929 |    0.888493603696927 |  0.6570857443423376 | 1753304138915 
 update2    |  29.47661370147079 | 0.010872312870502165 |  0.1593867607188556 | 1753577100603 
 update2    | 49.527694252432056 |   0.5142184937933181 |  0.7340133901254792 | 1753517338281 
 update2    |  9.384124531808036 |   0.6999655248704163 | 0.16603428449020086 | 1753523629384 
 update2    |  90.25710109008239 |   0.4006983139989222 | 0.08528650347654165 | 1753458728013 
 update2    |   90.9053809533154 |  0.19949323322922063 | 0.18294079059016366 | 1753653631225 
 update1    | 19.179139106643607 |   0.7528268153249502 |  0.8742041526408587 | 1753125594865 
 update1    |   43.4923811219014 |   0.8779402295427752 |  0.6100070562136587 | 1753163044342 
(10 rows)
```

We can also look in the Minio UI again to see the different files that have been created. Notice in the `.hoodie` path that we have two sets of `deltacommit` files

![MoR table update](../images/mor_dirs2.png)

Let's add data in the `spark-shell` one more time, this time specifying that we want to compact the MoR table after the second commit. This means that both the changes made in this operation and in the previous "insert" operation will be made "final".

```
val moreUpdates = convertToStringList(dataGen.generateUpdates(100))
val moreUpdatedData = spark.read.json(spark.sparkContext.parallelize(moreUpdates, 2));

moreUpdatedData.withColumn("commit_num", lit("update3")).write.format("hudi").
    options(getQuickstartWriteConfigs).
    options(hudiOptions(morTableName, "MERGE_ON_READ", "ts")).
    option("hoodie.compact.inline", "true").
    option("hoodie.compact.inline.max.delta.commits", "2").
    mode(Append).
    save(s"$basePath/$morTableName");
```

Now when we query both tables in the Presto CLI, we see that the RO and RT MoR tables are once again in line.

```sh
presto:default> select commit_num, fare, begin_lon, begin_lat, ts from mor_trips_table_ro;
 commit_num |        fare        |      begin_lon      |      begin_lat      |      ts       
------------+--------------------+---------------------+---------------------+---------------
 update3    | 26.636532270940915 | 0.12314538318119372 | 0.35527775182006427 | 1753628123984 
 update3    |  78.85334532337876 | 0.06330332057511467 | 0.16098476392187366 | 1753568217921 
 update3    |  58.09499619051147 | 0.49899171213436844 |  0.9692506010574379 | 1753614222174 
 update2    |   90.9053809533154 | 0.19949323322922063 | 0.18294079059016366 | 1753653631225 
 update3    | 53.682142277927525 |  0.9635314017496284 | 0.16258177392270334 | 1753560939974 
 update3    |  86.98901645001811 |  0.2853709038726113 |  0.9180654821797201 | 1753598176693 
 update3    |  84.14360533180016 | 0.18969854255968877 | 0.30523673273999896 | 1753635483683 
 update3    | 44.596839246210095 | 0.38697902072535484 |  0.9045189017781902 | 1753620381238 
 update3    | 14.824941686410531 |  0.9596221628238303 |  0.9983185001324134 | 1753638430900 
 update3    |  71.08018349571618 |  0.8150991077375751 | 0.01925237918893319 | 1753655212524 
(10 rows)

Query 20250727_223417_00005_h4y8p, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
[Latency: client-side: 287ms, server-side: 269ms] [10 rows, 426KB] [37 rows/s, 1.55MB/s]

presto:default> select commit_num, fare, begin_lon, begin_lat, ts from mor_trips_table_rt;
 commit_num |        fare        |      begin_lon      |      begin_lat      |      ts       
------------+--------------------+---------------------+---------------------+---------------
 update3    | 26.636532270940915 | 0.12314538318119372 | 0.35527775182006427 | 1753628123984 
 update3    |  78.85334532337876 | 0.06330332057511467 | 0.16098476392187366 | 1753568217921 
 update3    |  58.09499619051147 | 0.49899171213436844 |  0.9692506010574379 | 1753614222174 
 update3    |  84.14360533180016 | 0.18969854255968877 | 0.30523673273999896 | 1753635483683 
 update3    | 44.596839246210095 | 0.38697902072535484 |  0.9045189017781902 | 1753620381238 
 update3    | 14.824941686410531 |  0.9596221628238303 |  0.9983185001324134 | 1753638430900 
 update3    |  71.08018349571618 |  0.8150991077375751 | 0.01925237918893319 | 1753655212524 
 update2    |   90.9053809533154 | 0.19949323322922063 | 0.18294079059016366 | 1753653631225 
 update3    | 53.682142277927525 |  0.9635314017496284 | 0.16258177392270334 | 1753560939974 
 update3    |  86.98901645001811 |  0.2853709038726113 |  0.9180654821797201 | 1753598176693 
(10 rows)
```

In the Minio UI, we are able to see a third set of `deltacommit`s as well as the compaction commit.

![MoR compaction](../images/mor_dirs3.png)

## 4. Create CoW Hudi table

In this section we'll explore Hudi Copy-on-Write (CoW) tables. CoW tables store data using exclusively columnar file formats (e.g parquet). Updates version & rewrites the files by performing a synchronous merge during write. Let's create a COW table with partitions in Spark so that we can also see how partitioning changes the directory structure of our tables. From within the `spark-shell` session from the previous sections, enter the following code in paste mode:

```
val cowTableName = "cow_trips_table"

val countries = Seq("US", "IN", "DE")
val data = (1 to 500).map { i =>
  (UUID.randomUUID().toString(), s"user_$i", 20 + Random.nextInt(40), countries(Random.nextInt(countries.length)))
}.toDF("uuid", "name", "age", "country")

data.write.format("hudi")
  .options(getQuickstartWriteConfigs)
  .options(hudiOptions(cowTableName, "COPY_ON_WRITE", "age", Some("country")))
  .mode(Overwrite)
  .save(s"$basePath/$cowTableName")
```

In the MinIO UI, we can see that we now have a new table path - `cow_trips_table` - under `hudi_tables`. When we explore this path more, we can see the partition directories that have been created for this table according to our specified partition path, "country". Within each are the columnar data files that are associated with that partition.

![CoW table partitions](../images/cow_dirs.png)

Additionally, in the `.hoodie` directory, we can see that there is a single set of `commit` files. Notice how this differs from our MoR table, which creates `deltacommit` files.

## 5. Query CoW table with Presto

From our Presto CLI tab, we can query the new table. First verify that it has synced to the Hive metastore by running a `show tables` command:

```
presto:default> show tables;
       Table        
--------------------
 cow_trips_table    
 mor_trips_table_ro 
 mor_trips_table_rt 
(3 rows)
```

We can then run a `select` statement:

```
presto:default> select _hoodie_commit_time, _hoodie_partition_path, _hoodie_file_name, uuid, name, age, country from cow_trips_table limit 10;
 _hoodie_commit_time | _hoodie_partition_path |                             _hoodie_file_name                              |                 uuid                 |   name   | age | country 
---------------------+------------------------+----------------------------------------------------------------------------+--------------------------------------+----------+-----+---------
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | f2a61600-ddcb-4deb-8692-43c6fc3c7460 | user_319 |  49 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | fd13dca9-daf1-4f61-975e-21c914326347 | user_7   |  31 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | ebe3334a-cca0-4abe-955d-dd7e9f27cfbd | user_133 |  34 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | 59fd628a-eebe-436b-852f-6c248249029d | user_160 |  36 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | a0141e4a-1a23-4e92-8a5f-564d64b205d7 | user_208 |  30 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | ba89f773-109f-4f8c-aebe-6aa237d39b31 | user_41  |  45 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | 01e17303-89b7-4ffc-b95f-689018acf730 | user_383 |  37 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | e67bcdbc-0a57-4800-a9b5-57426eb82112 | user_317 |  56 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | 0594e246-7b62-4ddb-b442-8759864fa270 | user_39  |  51 | IN      
 20250729001157020   | country=IN             | c74bc599-44e3-43fd-9851-6ed16c7c0b96-0_1-111-120_20250729001157020.parquet | fa8d99e4-6100-41a1-b2c1-dec3a1688608 | user_373 |  45 | IN      
(10 rows)
```

Notice that you can see the relevant Hudi metadata information for each row of the data. Note: this information is also available for the MoR tables, but we chose to omit it in the previous section for brevity.

From here, you can experiment with adding data to our partitioned CoW table and exploring how the queries and s3 storage files change. You can also explore more advanced queries of the Hudi metadata on the MoR tables.
