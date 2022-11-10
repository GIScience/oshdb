package org.heigit.ohsome.oshdb.helpers.db;

import static org.heigit.ohsome.oshdb.helpers.db.Util.getInterpolated;

import com.zaxxer.hikari.HikariDataSource;
import java.util.Properties;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;

/**
 * A basic OSHDBDriver class for connecting to h2 or ignite oshdb instances.
 *
 * <pre>
 * {@code
 *     OSHDBDriver.connect(props, (oshdb) -> {
 *         oshdb.getSnapshotView()
 *         .areaOfInterest((Geometry & Polygonal) areaOfInterest)
 *         .timestamps(tstamps)
 *         .osmTag(key)
 *          ...
 *     });
 * }
 * </pre>
 *
 */
public class OSHDBDriver {

  public static final String OSHDB_PROPERTY_NAME = "oshdb";
  public static final String KEYTABLES_PROPERTY_NAME = "keytables";
  public static final String PREFIX_PROPERTY_NAME = "prefix";
  public static final String MULTITHREADING_PROPERTY_NAME = "multithreading";
  public static final String IGNITE_URI_PREFIX = "ignite:";

  private OSHDBDriver() {
    throw new IllegalStateException("Driver class");
  }

  /**
   * open connection to oshdb instance.
   *
   * @param props
   *     <p>
   *        props example:
   *     </p>
   *
   *     <pre>
   *     {@code
   *       oshdb="ignite:Path_To_Config" \ "h2:Path_To_Database"
   *       keytables="jdbc:postgresql://IP_OR_URL/keytables\${prefix}?user=ohsome&password=secret"
   *       #multithreading default false, only used by h2 connections
   *       multithreading=true
   *     }
   *     </pre>
   * @param connect A Consumer for a OSHDBConnection e.g. a lambda
   * @return exit code
   * @throws Exception when an Exception is thrown in the underlying classes
   */
  public static int connect(Properties props, Execute connect) throws Exception {
    var oshdb = getInterpolated(props, OSHDBDriver.OSHDB_PROPERTY_NAME)
        .orElseThrow(() ->  new IllegalArgumentException("need to have to specify oshdb!"));
    if (oshdb.toLowerCase().startsWith(IGNITE_URI_PREFIX)) {
      return connectToIgnite(props, connect);
    } else if (oshdb.toLowerCase().startsWith("h2:")) {
      return connectToH2(props, connect);
    } else {
      throw new IllegalArgumentException("unknown oshdb value! " + oshdb);
    }
  }

  private static int connectToH2(Properties props, Execute connect)
      throws Exception {
    var prefix = getInterpolated(props, PREFIX_PROPERTY_NAME).orElse("");
    props.put(PREFIX_PROPERTY_NAME, prefix);
    var h2 =
        getInterpolated(props, OSHDB_PROPERTY_NAME)
        .map(value -> value.substring("h2:".length())).orElseThrow();
    var multithreading =
        getInterpolated(props, MULTITHREADING_PROPERTY_NAME).filter("true"::equalsIgnoreCase)
            .isPresent();
    return connectToH2(h2, prefix, multithreading, connect);
  }

  // OSHDBJdbc throws "Exception"
  @SuppressWarnings("java:S112")
  private static int connectToH2(String h2, String prefix, boolean multithreading,
      Execute connect) throws Exception {
    try (final var oshdb = new OSHDBH2(h2)) {
      oshdb.prefix(prefix);
      oshdb.multithreading(multithreading);
      var props = new Properties();
      props.setProperty(OSHDBDriver.OSHDB_PROPERTY_NAME, h2);
      props.setProperty(PREFIX_PROPERTY_NAME, prefix);
      final var connection = new OSHDBConnection(props, oshdb);
      return connect.apply(connection);
    }
  }

  // OSHDBJdbc throws "Exception"
  @SuppressWarnings("java:S112")
  private static int connectToIgnite(Properties props, Execute connect)
      throws Exception {
    var cfg = getInterpolated(props, OSHDB_PROPERTY_NAME)
        .filter(value -> value.toLowerCase().startsWith(IGNITE_URI_PREFIX))
        .map(value -> value.substring(IGNITE_URI_PREFIX.length()))
        .orElseThrow();
    // start ignite
    try (var ignite = Ignition.start(cfg)) {
      var prefix = getInterpolated(props, PREFIX_PROPERTY_NAME).orElseGet(() -> getActive(ignite));
      props.put(PREFIX_PROPERTY_NAME, prefix);
      var keyTablesUrl = getInterpolated(props, OSHDBDriver.KEYTABLES_PROPERTY_NAME)
          .orElseThrow(() -> new IllegalArgumentException("missing keytables"));
      props.put(OSHDBDriver.KEYTABLES_PROPERTY_NAME, keyTablesUrl);
      // initialize data source for keytables
      try (var dsKeytables = new HikariDataSource();
          var oshdb = new OSHDBIgnite(ignite, dsKeytables)) {
        dsKeytables.setJdbcUrl(keyTablesUrl);
        oshdb.prefix(prefix);
        var connection = new OSHDBConnection(props, oshdb);
        return connect.apply(connection);
      }
    }
  }

  private static String getActive(Ignite ignite) {
    // TODO: extract "ohsome" string
    //       one possible solution: https://github.com/GIScience/oshdb/issues/108
    return ignite.<String, String>cache("ohsome").get("active");
  }

  /**
   * Internally used execute interface that applies the given connection.
   */
  @FunctionalInterface
  public interface Execute {
    @SuppressWarnings("java:S112")
    int apply(OSHDBConnection oshdb) throws Exception;
  }
}
