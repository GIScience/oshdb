package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.heigit.bigspatialdata.oshdb.OSHDB;

public class OSHDB_JDBC extends OSHDB {

  private final Connection _conn;
  private boolean useMultithreading = true;

  public OSHDB_JDBC(String classToLoad, String jdbcString) throws SQLException, ClassNotFoundException {
    this(classToLoad, jdbcString, "sa", "");
  }

  public OSHDB_JDBC(String classToLoad, String jdbcString, String user, String pw) throws SQLException, ClassNotFoundException {
    Class.forName(classToLoad);
    this._conn = DriverManager.getConnection(jdbcString, user, pw);
  }

  public OSHDB_JDBC(Connection conn) throws SQLException, ClassNotFoundException {
    this._conn = conn;
  }

  public Connection getConnection() {
    return this._conn;
  }

  public OSHDB_JDBC multithreading(boolean useMultithreading) {
    this.useMultithreading = useMultithreading;
    return this;
  }

  public boolean multithreading() {
    return this.useMultithreading;
  }
}
