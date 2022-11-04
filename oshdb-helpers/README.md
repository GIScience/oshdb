# OSHDB Driver and Application

[![build status](https://jenkins.ohsome.org/buildStatus/icon?job=oshdb-database-driver/master)](https://jenkins.ohsome.org/blue/organizations/jenkins/oshdb-database-driver/activity/?branch=master)
[![JavaDocs](https://img.shields.io/badge/Java-docs-blue.svg)](https://docs.ohsome.org/java/oshdb-database-driver)
[![status: experimental](https://github.com/GIScience/badges/raw/master/status/experimental.svg)](https://github.com/GIScience/badges#experimental)

Simple [OSHDB](https://github.com/GIScience/oshdb) connection helpers that automatically open an Ignite or H2 connection, depending on the input.

Two functionalities are available:

 - The [OSHDBApplication](doc/OSHDBApplication.md) (recommended usage) provides a full [Spring boot](https://spring.io/projects/spring-boot) -like application including a CLI. "Just add" your OSHDB functionality to create a usage-ready application. 
 - The [OSHDBDriver](doc/OSHDBDriver.md) provides a static method that exhibits an OSHDB connection to a respective `Consumer`. It leaves you with all setup work for your application and will only handle the OSHDB connection part.

## Installation

The tool is not (yet) available on Maven central. To add it to your project add our public repository in addition to the dependency:

```xml
<project>
   [...]
   <repositories>
      <repository>
         <id>oshdb-repository</id>
         <name>Heigit/GIScience maven repository</name>
         <url>https://repo.heigit.org/artifactory/main</url>
      </repository>
   </repositories>

   <dependencies>
      <dependency>
         <groupId>org.heigit.ohsome.oshdb.helpers</groupId>
         <artifactId>OSHDBDriver</artifactId>
         <version>1.0-SNAPSHOT</version>
      </dependency>
      [...]
   </dependencies>
</project>
```

##  Configuration

Both functionalities will need the following configuraiton options. The details how to specify them will be discussed in the respective subsection.

 - oshdb
    - for a connection to an H2-file this is the absolute path to the H2-file prefixed by "h2:" like "h2:/path/to/file.oshdb.mv.db"
    - for a connection to an Ignite cluster this is the absolute path to an ignite-config.xml file prefixed by "ignite:" like "ignite:/path/to/file.xml"
 - prefix (optional)
    - a string prefixed to database objects to allow multiple data versions to co-exist in the backend. It is (only) necessary if you want to access a legacy OSHDB ignite cluster. You will be notified about this once you get access to an H2-file or the Ignite cluster. If it is defined, it is mostly something like "global-xxxx" where xxxx is the sequence number from http://api.ohsome.org/v1/metadata
 - keytables (optional)
    - a JDBC string defining a connection to the [keytables](https://github.com/GIScience/oshdb/blob/83d337fbb5b2f923b29b8c56f182a883e922029b/documentation/manual/data-model.md#keytables) linked to the targeted OSHDB. It is (only) necessary if you want to access a legacy OSHDB ignite cluster. H2 files normally self contain the keytables where the tools are able to find them. The same is true for standard Ignite cluster backends.
 - multithreading (optional)
    - a boolean parameter for jdbc based connections (i.e. H2) if multithreading should be enabled during processing. 
 
 Note that any `${some-property}` (e.g. `${prefix}`) within these property strings will be automatically replaced by the respective property value. So you can safely include these placeholders into your keytables URL (or any other property), if needed.
