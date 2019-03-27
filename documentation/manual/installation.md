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



## Option 2: Building from Source

Alternatively, you may [build oshdb from source] using maven. This option
is more involved and requires more knowledge of building software with maven.
