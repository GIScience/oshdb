package org.heigit.ohsome.oshdb.api.db;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;

/**
 * OSHDB database backend connector to a H2 database.
 */
public class OSHDBH2 extends OSHDBJdbc {

  private JdbcConnectionPool connectionPool;

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
      connectionPool.dispose();
    } finally {
      super.close();
    }
  }
}
