# Introduction

## Presto-Hudi Workshop - Building an Open Data Lakehouse with Presto and Apache Hudi

Welcome to our workshop! In this workshop, you’ll learn the basics of Presto, the open-source SQL query engine, and it's support for Hudi. You’ll get Presto running locally on your machine and connect to an S3-based data source and a Hive metastore, which enables our Hudi integration. This is a beginner-level workshop for software developers and engineers who are new to Presto and Hudi. At the end of the workshop, you will understand how to integrate Presto with Hudi and MinIO and to understand Hudi's unique features.

The goals of this workshop are to show you:

* What is Apache Hudi and how to use it
* How to connect Presto to MinIO s3 storage and an Hudi-compatible Hive metastore using Docker
* How to take advantage of Hudi using Presto and why you would want to

### About this workshop

The introductory page of the workshop is broken down into the following sections:

* [Agenda](#agenda)
* [Compatibility](#compatibility)
* [Technology Used](#technology-used)
* [Credits](#credits)

## Agenda

|  |  |
| :--- | :--- |
| [Introduction](introduction/README.md) | Introduction to the technologies used |
| [Pre-work](pre-work/README.md) | Prerequisites for the workshop |
| [Lab 1: Set up an Open Lakehouse](lab-1/README.md) | Set up Presto & Spark clusters, a Hive metastore, and an s3 storage mechanism |
| [Lab 2: Create & Query Basic Hudi Tables](lab-2/README.md) | Set up a Hudi tables from the `spark-shell` and explore them in MinIO and Presto |
| [Lab 3: Explore Hudi Table & Query Types](lab-3/README.md) | Explore how to create and interact with different types of Hudi tables and queries (intermediate-level concepts) |

## Compatibility

This workshop has been tested on the following platforms:

* **Linux**: Ubuntu 22.04
* **MacOS**: M1 Mac

## Technology Used

* [Docker](https://www.docker.com/): A container engine to run several applications in self-contained containers.
* [Presto](https://prestodb.io/): A fast and Reliable SQL Engine for Data Analytics and the Open Lakehouse
* [Apache Hudi](https://hudi.apache.org/): A high-performance open table format to bring database functionality to your data lakes
* [Spark](https://spark.apache.org/): ! multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters
* [MinIO](https://min.io/): A high-performance, S3 compatible object store

## Credits

* [Kiersten Stokes](https://github.com/kiersten-stokes)
* Deepak Panda
