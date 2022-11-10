package org.heigit.ohsome.oshdb.api.db;

import static org.heigit.ohsome.oshdb.api.db.H2Support.pathToUrl;

import java.nio.file.Path;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * OSHDB database backend connector to a H2 database.
 */
public class OSHDBH2 extends OSHDBJdbc {

  private final JdbcConnectionPool connectionPool;

  /**
   * Opens a connection to oshdb data stored in a H2 database file.
   *
   * @param databaseFile the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   */
  public OSHDBH2(Path databaseFile) {
    this(databaseFile, "sa", "");
  }

  public OSHDBH2(String databaseFile) {
    this(Path.of(databaseFile));
  }

  public OSHDBH2(String url, String user, String password) {
    this(JdbcConnectionPool.create(url, user, password));
  }

  public OSHDBH2(Path path, String user, String password) {
    this(pathToUrl(path), user, password);
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
