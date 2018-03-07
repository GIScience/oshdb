package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OSHDBH2 extends OSHDBJdbc {

  public OSHDBH2(String databaseFile) throws SQLException, ClassNotFoundException {
    super("org.h2.Driver", "jdbc:h2:" + databaseFile.replaceAll("\\.mv\\.db$", "") + ";ACCESS_MODE_DATA=r");
  }
  
  public OSHDBH2(Connection conn) throws ClassNotFoundException, SQLException{
    super(conn);
  }

  public OSHDBH2 multithreading(boolean useMultithreading) {
    super.multithreading(useMultithreading);
    return this;
  }
  
  public OSHDBH2 cacheInMemory(boolean cache) throws ClassNotFoundException, SQLException {
    if(!cache)
      return this;
    
    Connection dest = DriverManager.getConnection("jdbc:h2:mem:");
    try (Connection src = this.getConnection()) {
      try (Statement srcStmt = src.createStatement(); ResultSet srcRst = srcStmt.executeQuery("script drop")) {
        try (Statement destStmt = dest.createStatement()) {
          while (srcRst.next()) {
            destStmt.executeUpdate(srcRst.getString(1));
          }
        }
      }
    }
    
    this._conn = dest;
    return this;
  }

}
