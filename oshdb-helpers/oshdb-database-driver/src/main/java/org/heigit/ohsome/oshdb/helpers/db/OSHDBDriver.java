package org.heigit.ohsome.oshdb.helpers.db;

import static org.heigit.ohsome.oshdb.helpers.db.Util.getInterpolated;

import java.sql.DriverManager;
import java.util.Properties;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;

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
   * @throws java.lang.Exception
   */
  public static int connect(Properties props, Execute connect) throws Exception {
    var oshdb = getInterpolated(props, "oshdb")
        .orElseThrow(() ->  new IllegalArgumentException("need to have to specifiy oshdb!"));
    if (oshdb.toLowerCase().startsWith("ignite:")) {
      return connectToIgnite(props, connect);
    } else if (oshdb.toLowerCase().startsWith("h2:")) {
      return connectToH2(props, connect);
    } else {
      throw new IllegalArgumentException("unknown oshdb value! " + oshdb);
    }
  }

  public static int connectToH2(Properties props, Execute connect)
      throws Exception {
    var prefix = getInterpolated(props, "prefix").orElse("");
    props.put("prefix", prefix);
    var h2 =
        getInterpolated(props, "oshdb")
        .map(value -> value.substring("h2:".length())).orElseThrow();
    var multithreading =
        getInterpolated(props, "multithreading").filter("true"::equalsIgnoreCase).isPresent();
    return connectToH2(h2, prefix, multithreading, connect);
  }

  public static int connectToH2(String url, String prefix, Execute connect)
      throws Exception {
    return connectToH2(url, prefix, true, connect);
  }

  // OSHDBJdbc throws "Exception"
  @SuppressWarnings("java:S112")
  public static int connectToH2(String h2, String prefix, boolean multithreading,
      Execute connect) throws Exception {
    try (final var oshdb = new OSHDBH2(h2);
        final var keyTables = new OSHDBJdbc(oshdb.getConnection())) {
      oshdb.prefix(prefix);
      oshdb.multithreading(multithreading);
      var props = new Properties();
      props.setProperty("oshdb", h2);
      props.setProperty("prefix", prefix);
      final var connection = new OSHDBConnection(props, oshdb, keyTables);
      return connect.apply(connection);
    }
  }

  // OSHDBJdbc throws "Exception"
  @SuppressWarnings("java:S112")
  private static int connectToIgnite(Properties props, Execute connect)
      throws Exception {
    var cfg = getInterpolated(props, "oshdb").filter(value -> value.toLowerCase().startsWith("ignite:"))
        .map(value -> value.substring("ignite:".length())).orElseThrow();
    try (var ignite = Ignition.start(cfg)) {
      var prefix = getInterpolated(props, "prefix").orElseGet(() -> getActive(ignite));
      props.put("prefix", prefix);
      var keyTablesUrl = getInterpolated(props, "keytables")
          .orElseThrow(() -> new IllegalArgumentException("missing keytables"));
      props.put("keytables", keyTablesUrl);
      try (var ktConnection = DriverManager.getConnection(keyTablesUrl);
          var keytables = new OSHDBJdbc(ktConnection);
          var oshdb = new OSHDBIgnite(ignite)) {
        oshdb.prefix(prefix);
        var connection = new OSHDBConnection(props, oshdb, keytables);
        return connect.apply(connection);
      }
    }
  }

  private static String getActive(Ignite ignite) {
    return ignite.<String, String>cache("ohsome").get("active");
  }

  @FunctionalInterface
  public interface Execute {
    int apply(OSHDBConnection oshdb) throws Exception;
  }
}
