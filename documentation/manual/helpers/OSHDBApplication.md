# OSHDB Application Template
## Installation

Replace your OSHDB dependency with the following:

```xml
<dependency>
   <groupId>org.heigit.ohsome</groupId>
   <artifactId>oshdb-application-template</artifactId>
   <version>1.2.0</version>
</dependency>
```

## Usage

Create a main Class extending on the `OSHDBApplication`:

```java
public class MyApplication extends OSHDBApplication {

    public static void main(String[] args) {}
    
    @Override
    protected int run(OSHDBConnection oshdb) throws Exception {
      return 0;
    }
}
```

Activate you application by executing it in the `main` method:

```java
    public static void main(String[] args) throws Exception {
        OSHDBApplication.run(MyApplication.class, args);
    }
```

The `run` method now replaces your `main` method. You can add any functionality starting from there. The return value represents the status code. 0 signals a successful execution and should be the default.

```java
    @Override
    protected int run(OSHDBConnection oshdb) throws Exception {
        // your code goes here
        return 0;
    }
```

Other output (e.g. from the OSHDB query itself), needs to be handled within the method like writing it to a file, storing it in a database or printing it to the screen.

To provide input to your application or query you can equally use the methods' body. Alternatively you can extend the predefined [picocli](https://picocli.info/)-based CLI with your own arguments

```java
    @CommandLine.Option(names = {"--myInput"}, description = "custom CLI input")
    protected String input;

    @Override
    protected int run(OSHDBConnection oshdb) throws Exception {
        System.out.println(this.input);
```

To run your application/query execute it in a command line. To configure the oshdb connection (see the [README.md](README.md)) you can either use the provided CLI options (`--oshdb`, `--prefix`, `--keytables`), provide a `.properties` file (`--props`), or both. Any given CLI option will overwrite the properties provided in the file (if both parameters are present).

## Example

```java
package mypackage;

import org.heigit.ohsome.oshdb.helpers.applicationtemplate.OSHDBApplication;
import org.heigit.ohsome.oshdb.helpers.db.OSHDBConnection;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import picocli.CommandLine;

public class MyApplication extends OSHDBApplication {

    public static void main(String[] args) throws Exception {
        OSHDBApplication.run(MyApplication.class, args);
    }

    @CommandLine.Option(defaultValue = "2018-05-01", names = {"--ts"}, description = "target timestamp, default=${DEFAULT-VALUE}")
    protected String ts;

    @Override
    protected int run(OSHDBConnection oshdb) throws Exception {
        OSHDBBoundingBox bbox = OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);

        Integer result = oshdb.getSnapshotView()
                .areaOfInterest(bbox)
                .filter("type:node")
                .timestamps(this.ts)
                .count();

        System.out.println(result);

        return 0;
    }

}
```

Run it e.g. with `mvn exec:java -Dexec.mainClass="mypackage.MyApplication" -Dexec.args="--oshdb h2:/path/to/file.oshdb.mv.db"`.
