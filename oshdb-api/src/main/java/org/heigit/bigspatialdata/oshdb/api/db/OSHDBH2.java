package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OSHDBH2 extends OSHDBJdbc {

  public static OSHDBH2 get(String databaseFile, boolean inMemory) throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    
    String url = "jdbc:h2:" + databaseFile.replaceAll("\\.mv\\.db$","") + ";IFEXISTS=TRUE;ACCESS_MODE_DATA=r";
    
    if(!inMemory){
      return new OSHDBH2(DriverManager.getConnection(url, "sa", ""));
    }
    
    Connection dest = DriverManager.getConnection("jdbc:h2:mem:");
    try (Connection src = DriverManager.getConnection(url, "sa", "")) {
      try (Statement srcStmt = src.createStatement(); ResultSet srcRst = srcStmt.executeQuery("script")) {
        try (Statement destStmt = dest.createStatement()) {
          while (srcRst.next()) {
            destStmt.executeUpdate(srcRst.getString(1));
          }
        }
      }
    }
    return new OSHDBH2(dest);
  }

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

}
