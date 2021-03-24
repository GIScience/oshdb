Installation
============

## Configuring Java Version

Compiling software that includes the OSHDB currently requires Java 11 (i.e., version 11) or newer. Because maven projects use Java 1.5 by default, it is necessary to either configure your IDE accordingly or add the following compiler version to the properties section of your pom.xml file:

```
<properties>
  <maven.compiler.source>11</maven.compiler.source>
  <maven.compiler.target>11</maven.compiler.target>
</properties>
```

## Option 1: Adding the OSHDB as a maven dependency 

Simply add the OSHDB as a dependency to your `pom.xml` file. For most use cases this would be the [`oshdb-api`](api.md):

```xml
<dependencies>
  <dependency>
    <groupId>org.heigit.ohsome</groupId>
    <artifactId>oshdb-api</artifactId>
    <version>0.6.4</version>
  </dependency>
</dependencies>
```

Advaced users may also be interested in other packages like the `oshdb` package for raw data access.

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
