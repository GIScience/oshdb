# Setup a Local Oshdb

In order to create a local database instance of the oshdb yourself, you
need a .osh.pbf-file of your area of interest. You can get one, e.g., from
[geofabrik](http://download.geofabrik.de/). The oshdb instance may then
be created by the steps extract, transform and load as follows:

## Extract

The first step is to extract data by from your .osh.pbf-File. To do so,
you have to figure out the valid time period contained in this file and
to provide the start of this period in the ISO date-time format. The
actual extraction is performed by running the following commands, assuming
that you are in the base directory of the downloaded
[OSH-Code](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/oshdb/core/tree/master).


```bash
cd oshdb-tool/etl
mvn exec:java -Dexec.mainClass="org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract" -Dexec.args="--pbf /absolute/path/to/file.osh.pbf -tmpDir ./tmpFiles --timevalidity_from YYYY-MM-DD"
```

This creates the files `extract_keys`, `extract_keyvalues` and `extract_roles` 
containing the keys and the values of tags and the roles of relations.

For large files, you might have to increase the size of your JVM by executing
`export MAVEN_OPTS="-Xmx???"` (replace ??? with a reasonable size for your machine)
before extracting.

Run `mvn exec:java -Dexec.mainClass="org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract"`
to get help and more options.


## Transform

After extraction, a transformation step creates the actual Oshdb using
the H2 database engine:

```bash
mvn exec:java -Dexec.mainClass="org.heigit.bigspatialdata.oshdb.tool.importer.transform.Transform" -Dexec.args="--pbf /absolute/path/to/file.osh.pbf -tmpDir ./tmpFiles"
```
The transformation step is computation-intensive, so be easy on your computer
and do not use too large files.


## Load

### Load into an H2 Database

The transformed data has to be loaded into a database to which the oshdb will interface.
In order to enable the oshdb to provide a proper attribution of the imported data, you
have to set an attribution text and an attribution url

```bash
mvn exec:java -Dexec.mainClass="org.heigit.bigspatialdata.oshdb.tool.importer.load.handle.OSHDB2H2Handler" -Dexec.args="-tmpDir ./tmpFiles --out /absolote/path/to/your-H2-database --attribution '© OpenStreetMap contributors' --attribution-url 'https://www.openstreetmap.org/copyright'"
```

You now have a ready-to-use oshdb named **your-H2-database.mv.db** in the specified
output directory (the file extension .mv.db is appended automatically).


### Deploy on Apache Ignite (optional)

If you wish to deploy the Oshdb on [Apache ignite](https://ignite.apache.org),
the previously created H2 database has to be loaded into ignite. You can 
[download ignite](https://ignite.apache.org/download.cgi#binaries) from the
Apache website.

1. In order to deploy the oshdb dependencies to ignite,
   change back to the base directory and let maven copy all dependencies to
   your ignite installation. It assumes this is at /opt/ignite. If it is not,
   you may place a link to the actual location there.<br>
   ```bash
   cd ../..
   mvn -Pdeployignite clean install
   ```

2. Then start as many ignite servers in a pseudo-distributed system as you
   think your computer can handle using an appropriate ignite-config.xml.
   Ignite's default configuration file is located at
   /opt/ignite/config/default-config.xml.
   You either have to use a terminal for each ignite server or nohup:<br>
   ```bash
   nohup /opt/ignite/bin/ignite.sh ignite-config.xml &>/opt/ignite/log.log &
   /opt/ignite/bin/ignite.sh ignite-config.xml
   ```

3. Finally, you can import the data as follows using
   the same ignite-config as in the previous step (note that the file extension .mv.db
   has to be omitted):<br>
   ```bash
   cd oshdb-tool/etl
   mvn exec:java -Dexec.mainClass="org.heigit.bigspatialdata.oshdb.tool.importer.util.OSHDB2Ignite" -Dexec.args="-ignite ignite-config.xml -db /absolute/path/to/your-H2-database"
   ```

