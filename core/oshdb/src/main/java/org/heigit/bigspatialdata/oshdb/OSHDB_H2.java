package org.heigit.bigspatialdata.oshdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OSHDB_H2 extends OSHDB {
  private final Connection _conn;
  private boolean useMultithreading = true;

  public OSHDB_H2(String databaseFile) throws SQLException, ClassNotFoundException {
    Class.forName("org.h2.Driver");
    this._conn = DriverManager.getConnection("jdbc:h2:" + databaseFile, "sa", "");
  }
  
  public Connection getConnection() {
    return this._conn;
  }

  public OSHDB_H2 multithreading(boolean useMultithreading) {
    this.useMultithreading = useMultithreading;
    return this;
  }

  public boolean multithreading() {
    return this.useMultithreading;
  }
}
