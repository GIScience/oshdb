# Setup a local Oshdb

In order to create a local database instance of the oshdb yourself, you
need a .osh.pbf-file of your area of interest. You can get one, e.g., from 
[geofabrik](http://download.geofabrik.de/). The oshdb instance may then
be created by the steps extract, transform and (optional) load as follows:
 
## Extract

The first step is to extract data by from your .osh.pbf-File. To do so,
run the following commands, assuming that you are in the base directory 
of the downloaded 
[OSH-Code](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/oshdb/core/tree/master):


```bash
cd oshdb-tool/etl
mvn exec:java -Dexec.mainClass="org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract" -Dexec.args="--pbf /absolute/path/to/file.osh.pbf -tmpDir ./tmpFiles"
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
and do not use too large files. You now have a ready-to-use Oshdb named 
**./oshdb.mv.db** in the current directory. 


## Load (optional)

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
   think your computer can handle using the provided ignite-config.xml.
   You either have to use a terminal for each ignite server or nohup:<br>
   ```bash
   nohup /opt/ignite/bin/ignite.sh ignite-config.xml &>/opt/ignite/log.log &
   /opt/ignite/bin/ignite.sh ignite-config.xml
   ```

3. Finally, you can import the data as follows:<br>
   ```bash
   cd oshdb-tool/etl
   mvn exec:java -Dexec.mainClass="org.heigit.bigspatialdata.oshdb.etl.load.OSHDB2Ignite" -Dexec.args="-ignite ../ignite-config.xml"
   ```

