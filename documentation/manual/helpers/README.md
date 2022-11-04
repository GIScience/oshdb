# OSHDB Helpers

Simple OSHDB connection helpers that automatically open an Ignite or H2 connection, depending on the input.

Two functionalities are available:

 - The [OSHDBApplication](OSHDBApplication.md) (recommended usage) provides a full [Spring boot](https://spring.io/projects/spring-boot)-like application including a CLI. "Just add" your OSHDB functionality to create a usage-ready application.
 - The [OSHDBDriver](OSHDBDriver.md) provides a static method that exhibits an OSHDB connection to a respective `Consumer`. It leaves you with all setup work for your application and will only handle the OSHDB connection part.


##  Configuration

Both functionalities will need the following configuration options. The details how to specify them will be discussed in the respective subsection.

 - `oshdb`
    - for a connection to an H2-file this is the absolute path to the H2-file prefixed by `h2:` like `h2:/path/to/file.oshdb.mv.db`
    - for a connection to an Ignite cluster this is the absolute path to an ignite-config.xml file prefixed by `ignite:` like `ignite:/path/to/file.xml`
 - `prefix` (optional)
    - a string prefixed to database objects to allow multiple data versions to co-exist in the backend. It is (only) necessary if you want to access a legacy OSHDB ignite cluster. You will be notified about this once you get access to an H2-file or the Ignite cluster.
 - `keytables` (optional for H2)
    - a JDBC string defining a connection to the [keytables](../data-model.md#keytables) linked to the targeted OSHDB. H2 files normally self contain the keytables where the tools are able to find them.
 - `multithreading` (optional for H2)
    - a boolean parameter for jdbc based connections (i.e. H2) if multithreading should be enabled during processing. 
 
 Note that any `${some-property}` (e.g. `${prefix}`) within these property strings will be automatically replaced by the respective property value. So you can safely include these placeholders into your keytables URL (or any other property), if needed.
