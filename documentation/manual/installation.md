Installation
============

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

Two entries need to be added to the pom.xml to set up the OSHDB as a dependency.

First the HeiGIT reopsitory needs to be defined to enable Maven to fetch the necessary `.jar` files:

```xml
<repository>
  <id>oshdb-releases</id>
  <name>Heigit/GIScience repository</name>
  <url>http://repo.heigit.org/artifactory/libs-release-local</url>
</repository>
```

In a second step the desired dependency is declared. For most use cases this would be the [`oshdb-api`](api.md):

```xml
<dependencies>
  <dependency>
    <groupId>org.heigit.bigspatialdata</groupId>
    <artifactId>oshdb-api</artifactId>
    <version>0.5.10</version>
  </dependency>
</dependencies>
```

Advaced users may also be interested in other packages like the `oshdb` package for raw data access. A complete list of OSHDB artifacts can be found in our [artifactory repository](http://repo.heigit.org/artifactory/webapp/#/artifacts/browse/tree/General/libs-release-local/org/heigit/bigspatialdata).


## Option 2: Building from Source

Alternatively, you may build oshdb from source using maven. This option is more involved and requires more knowledge of building software with maven.

The basic steps are to clone this repository:

```
git clone https://github.com/GIScience/oshdb.git
```

and to build it with:

```
cd ./oshdb
mvn clean install
```

After that, the binaries will be available in the local maven directory. A new OSHDB-Project can be set up directly by defining the dependency as explained in the previous section [option 1](#option-1:-adding-the-oshdb-as-a-maven-dependency), step 2.
