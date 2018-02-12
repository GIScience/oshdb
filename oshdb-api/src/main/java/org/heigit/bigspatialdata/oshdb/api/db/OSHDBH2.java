package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.SQLException;

public class OSHDBH2 extends OSHDBJdbc {

  public OSHDBH2(String databaseFile) throws SQLException, ClassNotFoundException {
    super("org.h2.Driver", "jdbc:h2:" +
        databaseFile.replaceAll("\\.mv\\.db$", "") + ";ACCESS_MODE_DATA=r");
  }

  public OSHDBH2 multithreading(boolean useMultithreading) {
    super.multithreading(useMultithreading);
    return this;
  }

}
