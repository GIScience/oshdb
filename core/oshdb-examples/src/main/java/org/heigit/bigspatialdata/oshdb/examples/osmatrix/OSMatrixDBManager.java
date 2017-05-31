package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class OSMatrixDBManager {
  
  private Connection conn = null;
  
  
  private void setConn(Connection conn) {
    this.conn = conn;
  }
  
  public Connection getConn() {
    return this.conn;
  }

  public OSMatrixDBManager() {
    super();
    // TODO Auto-generated constructor stub
  }

  public Connection createOSMatrixDBConnection() {
    
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:postgresql://lemberg.geog.uni-heidelberg.de:5432/osmatrixhd", "osmatrix", "osmatrix2016");
      setConn(conn);
      System.out.println("Hi 5. Connection established.");
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      System.err.println("Connection failed.");
      e.printStackTrace();
    }
    
    
    return conn;
    
  }
  
  
  public void insertOSMatrixTimestamp(java.util.Date date) {
    Connection conn = getConn();
    try {
      final PreparedStatement pstmt = conn.prepareStatement("INSERT INTO times (time) VALUES(?);");
      pstmt.setDate(1, new java.sql.Date( date.getTime()));
      pstmt.execute();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
        
  }
  public int truncateTimesTable(){
    try (Connection connection = getConn();
        Statement statement = connection.createStatement()) {
     int result = statement.executeUpdate("TRUNCATE " + "times" + " CASCADE");
     connection.commit();
     return result;
   } catch (SQLException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
  }
    return 0;
    
  }

}
