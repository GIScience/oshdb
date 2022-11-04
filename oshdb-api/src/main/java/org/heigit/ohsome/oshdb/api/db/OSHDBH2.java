package org.heigit.ohsome.oshdb.api.db;

import static java.util.Optional.ofNullable;

import java.nio.file.Path;
import javax.sql.DataSource;
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

  public OSHDBH2(DataSource ds) {
    super(ds);
    this.connectionPool = null;
  }

  private OSHDBH2(JdbcConnectionPool ds) {
    super(ds);
    this.connectionPool = ds;
  }

  private static String pathToUrl(Path path) {
    var absolutePath = path.toAbsolutePath().toString();
    absolutePath = absolutePath.replaceAll("\\.mv\\.db$", "");
    return String.format("jdbc:h2:%s;ACCESS_MODE_DATA=r", absolutePath);
  }

  @Override
  public OSHDBH2 prefix(String prefix) {
    return (OSHDBH2) super.prefix(prefix);
  }

  @Override
  public OSHDBH2 multithreading(boolean useMultithreading) {
    return (OSHDBH2) super.multithreading(useMultithreading);
  }

  @Override
  public void close() throws Exception {
    try {
      ofNullable(connectionPool).ifPresent(JdbcConnectionPool::dispose);
    } finally {
      super.close();
    }
  }
}
