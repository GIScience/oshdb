package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.SQLException;

public class OSHDB_H2 extends OSHDB_JDBC {

  public OSHDB_H2(String databaseFile) throws SQLException, ClassNotFoundException {
    super("org.h2.Driver", "jdbc:h2:" + databaseFile);
  }
  
}
