Installation
============

> This page is currently missing some information. 

## Configuring Java Version

Compiling software that includes the OSHDB currently requires Java 8
(i.e., version 1.8) or newer. Because maven projects use Java 1.5 by default, it is necessary to either configure your IDE accordingly or add the following
compiler version to the properties section of your pom.xml file:

```
<properties>
  <maven.compiler.source>1.8</maven.compiler.source>
  <maven.compiler.target>1.8</maven.compiler.target>
</properties>
```

## Option 1: Adding the OSHDB as a maven dependency 

Two inromations need to be added to the pom.xml to set up the OSHDB as a dependency.

First the HeiGIT reopsitory needs to be defined to enable Maven to fetch the necessary `.jar`s':

```xml
<repository>
  <snapshots>
    <enabled>false</enabled>
  </snapshots>
  <id>oshdb-releases</id>
  <name>Heigit/GIScience repository (releases)</name>
  <url>http://repo.heigit.org/artifactory/libs-release-local</url>
</repository>
```

In a second step the desired dependency is declared. For most users this would be the [`oshdb-api`](api.md) which is a well documented and user friendly abstraction of the OSHDB.
```xml
<dependencies>
  <dependency>
    <groupId>org.heigit.bigspatialdata</groupId>
    <artifactId>oshdb-api</artifactId>
    <version>0.5.0</version>
  </dependency>
</dependencies>
```

Advaced users may also be interested in other packages like the `oshdb` package for raw data access. A complete list of released Artifacts can be found in our [Artifactory repository](http://repo.heigit.org/artifactory/webapp/#/artifacts/browse/tree/General/libs-release-local/org/heigit/bigspatialdata)


## Option 2: Building from Source

Alternatively, you may build oshdb from source using maven. This option
is more involved and requires more knowledge of building software with maven.

The basic steps are to clone this repository

```
git clone https://github.com/GIScience/oshdb.git
```

and build it

```
cd ./oshdb
mvn clean install
```

After that, the binaries will be available in the local maven repository. A new OSHDB-Project can be set up directly by defining the dependency as explained in Option 1 step 2.
