# OSHDBDriver
## Installation

Replace your OSHDB dependency with the following:

```xml
<dependency>
   <groupId>org.heigit.ohsome</groupId>
   <artifactId>oshdb-database-driver</artifactId>
   <version>1.1.2</version>
</dependency>
```

## Usage

To connect to the OSHDB using the OSHDBDriver you first have to define the configuration like:

```java
    Properties props = new Properties();
    // For H2:
    props.setProperty("oshdb", "h2:PATH_TO_H2");
    // Or for Ignite:
    props.setProperty("oshdb","ignite:PATH_TO_CFG");
    props.setProperty("keytables","jdbc:postgresql://localhost/keytables-${prefix}?user=ohsome&password=secret");
```

Alternatively you can read-in a `….properties` file with this information e.g. like so:

```java
    Properties props = new Properties();
    try(Reader reader = new FileReader("oshdb.properties")){
          props.load(reader);
    }
```

Then connect to OSHDB using the driver like:

```java
    OSHDBDriver.connect(props, (OSHDBConnection oshdb) -> {
        // your oshdb code goes here
        return 0;
    });
```

## Example

```java
package mypackage;

import java.util.Properties;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.helpers.db.OSHDBDriver;

public class OSHDBDriverExample {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        // For H2:
        props.setProperty("oshdb", "h2:PATH_TO_H2");
        // Or for Ignite:
        props.setProperty("oshdb", "ignite:PATH_TO_CFG");
        props.setProperty("keytables", "jdbc:postgresql://localhost/keytables-global?user=ohsome&password=secret");

        OSHDBDriver.connect(props, oshdb -> {
            OSHDBBoundingBox bbox = OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);

            Integer result = oshdb.getSnapshotView()
                    .areaOfInterest(bbox)
                    .filter("type:node")
                    .timestamps("2018-05-01")
                    .count();

            System.out.println(result);
            return 0;
        });
    }

}
```
