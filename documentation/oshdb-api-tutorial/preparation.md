# Preparation

In this step, we explain how to setup your build-environment and
a main-class as a starting point for implementing your analysis.
We assume basic knowledge of Java, Maven and your IDE.

## Creating a Maven Java Project

First you need to create a new maven java project.

## Declaring Dependencies

Add the following dependency to the dependencies section of your
[pom.xml](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html),
where `${oshdb-version}` has to be replaced by the desired
version of the oshdb-API.

```
<dependencies>
  <dependency>
    <groupId>org.heigit.bigspatialdata</groupId>
    <artifactId>oshdb-api</artifactId>
    <version>${oshdb-version}</version>
    <type>jar</type>
  </dependency>
</dependencies>
```

## Installing the oshdb Code

There are two options for installing the oshdb code.

### Option 1 (recommended): Adding the HeiGIT repository

This option is the recommended way of accessing the oshdb code. However,
the HeiGIT-repository is currently only available within the Heidelberg
University network (VPN). You need to add the following repositories to
the repositories-section of your pom.xml:

```
<repositories>
  <repository>
      <id>HeiGIT main</id>
      <name>Central Repository for OSHDB dependency related artefacts</name>
      <url>http://repo.heigit.org/artifactory/main</url>
  </repository>

  <repository>
    <snapshots>
      <enabled>false</enabled>
    </snapshots>
    <id>oshdb-releases</id>
    <name>Heigit/GIScience maven repository (releases)</name>
    <url>http://repo.heigit.org/artifactory/libs-release-local</url>
  </repository>

  <repository>
    <snapshots />
    <id>oshdb-snapshots</id>
    <name>Heigit/GIScience maven repository (snapshots)</name>
    <url>http://repo.heigit.org/artifactory/libs-snapshot-local</url>
  </repository>
</repositories>
```

### Option 2: Building from Source

Alternatively, you may [build oshdb from source] using maven. This option
is more involved and requires more knowledge of building software with maven.

## Configuring Java Version

Compiling software that includes oshdb currently requires Java 8
(i.e., version 1.8). Because maven uses Java 1.5 by default, it
is necessary that you either configure your IDE or add the following
compiler version to the properties section of your pom.xml file:

```
<properties>
    <maven.compiler.source>1.8</maven.compiler.source>
	<maven.compiler.target>1.8</maven.compiler.target>
</properties>
```

## Getting a Database

In order to use the oshdb-API you need an acual database that you
can query. You have several options:

* [Download an H2-database] from our database repository (recommended for this tutorial),
* [Create your own local H2-database],
* [Deploy oshdb on ignite]. (not covered in this tutorial)

## Preparing the Main Class

First, create a new class with a main-method that will be used to trigger
your analysis. In the main-method, declare the link to your database
(note that when using the OSHDB_H2 backend, one has to omit the file extension
".mv.db" from the actual file name):

```
// -- PREPARATION --
// declare and open link to OSHDB
OSHDBDatabase oshdb = new OSHDBH2("path/to/extract.oshdb");
```

### Contribution View or Snapshot View?

The oshdb-API currently supports the following views on the data:

* The **snapshot view** processes the OSM elements that were current at a
  given point in time.
* The **contribution view** processes OSM elements at the points in time
  where an actual contribution (e.g., creation, change, deletion) happend
  within a given time period.

We will use a snaphot view in this tutorial but contribution views are
used analogously:

```
// create MapReducer
MapReducer<OSMEntitySnapshot> mapReducer = OSMEntitySnapshotView.on(oshdb);
// or
MapReducer<OSMContribution> mapReducerContribution = OSMContributionView.on(oshdb);
```

## Summary

After the preparation step, your project should be similar to
[pom.xml](example-pom.xml) and [OshdbApiTutorial.java](OshdbApiTutorial.java).
TODO: update the links

The next step is to setup the [mandatory settings](mandatory-settings.md).
