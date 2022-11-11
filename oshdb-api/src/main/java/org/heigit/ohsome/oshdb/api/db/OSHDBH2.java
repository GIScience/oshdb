package org.heigit.ohsome.oshdb.api.db;

import static org.heigit.ohsome.oshdb.api.db.H2Support.createJdbcPool;
import static org.heigit.ohsome.oshdb.api.db.H2Support.createJdbcPoolFromPath;

import java.nio.file.Path;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * OSHDB database backend connector to a H2 database.
 */
public class OSHDBH2 extends OSHDBJdbc {

  public static final String DEFAULT_USER = "sa";
  public static final String DEFAULT_PASSWORD = "";
  private final JdbcConnectionPool connectionPool;

  /**
   * Opens a connection to oshdb data stored in a H2 database file.
   *
   * @param databaseFile the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   */
  public OSHDBH2(Path databaseFile) {
    this(createJdbcPoolFromPath(databaseFile));
  }

  /**
   * Opens a connection to oshdb data stored in a H2 database file.
   *
   * @param databaseFile the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   */
  public OSHDBH2(String databaseFile) {
    this(createJdbcPoolFromPath(databaseFile));
  }

  /**
   * Opens a connection to oshdb data stored in a H2 database file.
   *
   * @param url the JDBC URL to the H2 database file.
   * @param user username for the H2 JDBC connection
   * @param password password for the H2 JDBC connection
   */
  public OSHDBH2(String url, String user, String password) {
    this(createJdbcPool(url, user, password));
  }

  /**
   * Opens a connection to oshdb data stored in a H2 database file.
   *
   * @param path the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   * @param user username for the H2 JDBC connection
   * @param password password for the H2 JDBC connection
   */
  public OSHDBH2(Path path, String user, String password) {
    this(createJdbcPoolFromPath(path, user, password));
  }

  private OSHDBH2(JdbcConnectionPool ds) {
    super(ds, "");
    this.connectionPool = ds;
  }

  @Override
  public OSHDBH2 multithreading(boolean useMultithreading) {
    return (OSHDBH2) super.multithreading(useMultithreading);
  }

  @Override
  public void close() throws Exception {
    try {
      connectionPool.dispose();
    } finally {
      super.close();
    }
  }
}
