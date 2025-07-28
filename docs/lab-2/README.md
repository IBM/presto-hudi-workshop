# Set up the Data Source

In this section, you will create Hudi tables with Spark.

This section is comprised of the following steps:

- [Set up the Data Source](#set-up-the-data-source)
  - [1. Create Hudi tables](#1-create-hudi-tables)

## 1. Create Hudi tables

Start a Spark shell

```sh
docker exec -it spark /opt/spark/bin/spark-shell
```

Wait for it to start up

Enter "paste" mode by typing the following and pressing enter:

```sh
>scala :paste

// Entering paste mode (ctrl-D to finish)
```

Copy and paste the below code, which imports required packages, creates a spark session, and defines some variables that we will reference in subsequent code.

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

Press `Ctrl+D` to begin executing the pasted code.

We will complete the same process with our next code block, which will create and populate our data tables with randomly generated data about taxi trips.

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

Go to MinIO UI to explore the created file and directory structure. Looks similar right now with subtle differences in the `.hoodie` directory.

In a new terminal tab or window, go to Presto CLI to look at the tables and compare queries.

```sh
 docker exec -it coordinator presto-cli
```

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

Notice how Hudi has implicity created two versions of the MoR table, each provides a different view. But for now, querying them shows the same view since all tables are currently in the same state.

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

Now, let's go back to our spark-shell terminal tab and add more data to our tables using paste mode.

```
val updates = convertToStringList(dataGen.generateUpdates(10))
val updatedData = spark.read.json(spark.sparkContext.parallelize(updates, 2));

updatedData.withColumn("commit_num", lit("update2")).write.format("hudi").
    options(getQuickstartWriteConfigs).
    options(hudiOptions(morTableName, "MERGE_ON_READ", "ts")).
    mode(Append).
    save(s"$basePath/$morTableName");
```

Back to the Presto CLI, we see that the MoR RO and RT tables are starting to look different. The RT table has the freshest data, and the RO table still shows our previous state.

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

We can also look in the Minio UI again to see the different files that are created for each table.

Let's add data in the spark-shell one more time, this time specifying that we want to compact the MoR table after the second commit.

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

Now when we query both tables in the Presto CLI, we see that the RO and RT MoR tables are once again in line. Check the Minio UI to see that we've created a compaction commit.

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

Now let's create a COW table with partitions in Spark.

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

We can explore the partition directories in the Minio UI. And query in the Presto CLI.

```
presto:default> select * from cow_trips_table limit 10;
```

