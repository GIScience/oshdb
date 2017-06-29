package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class OshDBManager {
  private String connString;
  private String userName;
  private String password;
  private static final Logger logger = Logger.getRootLogger();
  
    
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
      logger.info("OshDB Driver successfully loaded");

      
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      System.err.println("Connection failed.");
      e.printStackTrace();
    }
    
    
    return conn;
    
  }
}
