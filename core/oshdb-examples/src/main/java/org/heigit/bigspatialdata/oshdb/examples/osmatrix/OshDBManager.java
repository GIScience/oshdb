package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OshDBManager {
  private String connString;
  private String userName;
  private String password;
  
    
  public OshDBManager(String connString, String userName, String password) {
    super();
    this.connString = connString;
    this.userName = userName;
    this.password = password;
  }


  public Connection createOshDBConnection() {
    
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(this.connString, this.userName, this.password);
     
      System.out.println("Hi 5. Connection to h2 established.");
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      System.err.println("Connection failed.");
      e.printStackTrace();
    }
    
    
    return conn;
    
  }
}
