
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
