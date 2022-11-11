package org.heigit.ohsome.oshdb.api.db;

import static org.heigit.ohsome.oshdb.api.db.OSHDBH2.DEFAULT_PASSWORD;
import static org.heigit.ohsome.oshdb.api.db.OSHDBH2.DEFAULT_USER;

import java.nio.file.Path;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * Support class for creating H2 JdbcConnectionPools.
 */
public class H2Support {

  private H2Support() {}

  /**
   * Creates a JdbcConnectionPool for an H2 file.
   *
   * @param databaseFile the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   */
  public static JdbcConnectionPool createJdbcPoolFromPath(String databaseFile) {
    return createJdbcPoolFromPath(databaseFile, DEFAULT_USER, DEFAULT_PASSWORD);
  }

  /**
   * Creates a JdbcConnectionPool for an H2 file.
   *
   * @param databaseFile the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   */
  public static JdbcConnectionPool createJdbcPoolFromPath(Path databaseFile) {
    return createJdbcPoolFromPath(databaseFile, DEFAULT_USER, DEFAULT_PASSWORD);
  }

  /**
   * Creates a JdbcConnectionPool for an H2 file.
   *
   * @param path the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   * @param user username for the H2 JDBC connection
   * @param password password for the H2 JDBC connection
   */
  public static JdbcConnectionPool createJdbcPoolFromPath(String path, String user,
      String password) {
    return createJdbcPool(pathToUrl(path), user, password);
  }

  /**
   * Creates a JdbcConnectionPool for an H2 file.
   *
   * @param path the file name and path to the H2 database file. (the ".mv.db" file ending
   *        of H2 should be omitted here)
   * @param user username for the H2 JDBC connection
   * @param password password for the H2 JDBC connection
   */
  public static JdbcConnectionPool createJdbcPoolFromPath(Path path, String user, String password) {
    return createJdbcPool(pathToUrl(path), user, password);
  }

  /**
   * Creates a JdbcConnectionPool for an H2 file.
   *
   * @param url the JDBC URL to the H2 database file.
   * @param user username for the H2 JDBC connection
   * @param password password for the H2 JDBC connection
   */
  public static JdbcConnectionPool createJdbcPool(String url, String user, String password) {
    return JdbcConnectionPool.create(url, user, password);
  }

  private static String pathToUrl(String path) {
    return pathToUrl(Path.of(path));
  }

  private static String pathToUrl(Path path) {
    var processedPath = path.toString().replaceFirst("^~", System.getProperty("user.home"));
    processedPath = Path.of(processedPath).toAbsolutePath().toString();
    processedPath = processedPath.replaceAll("\\.mv\\.db$", "");
    return String.format("jdbc:h2:%s;ACCESS_MODE_DATA=r", processedPath);
  }
}
