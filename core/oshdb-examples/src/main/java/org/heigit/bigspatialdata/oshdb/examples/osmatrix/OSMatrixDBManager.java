package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class OSMatrixDBManager {
  
  private String connString;
  private String userName;
  private String password;
  
    
  public OSMatrixDBManager(String connString, String userName, String password) {
    super();
    this.connString = connString;
    this.userName = userName;
    this.password = password;
  }


  public Connection createOSMatrixDBConnection() {
    
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(this.connString, this.userName, this.password);
     
      System.out.println("Hi 5. Connection established.");
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      System.err.println("Connection failed.");
      e.printStackTrace();
    }
    
    
    return conn;
    
  }
  
  
  public void insertOSMatrixTimestamp(java.util.Date date) {
    Connection conn = createOSMatrixDBConnection();
    try {
      final PreparedStatement pstmt = conn.prepareStatement("INSERT INTO times (time) VALUES(?);");
      pstmt.setDate(1, new java.sql.Date( date.getTime()));
      pstmt.execute();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
        
  }
  public void truncateTimesTable(){
    Connection connection = null;
    try {
      connection = createOSMatrixDBConnection();
      Statement statement = connection.createStatement();
      statement.execute("TRUNCATE " + "times" + " CASCADE");
      
      Statement resetSequence = connection.createStatement();
      resetSequence.execute("ALTER SEQUENCE times_id_seq RESTART");
  
   } catch (SQLException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
 
    }
    finally{
      
      try {
        connection.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  
  }
  public void fillTimesTable(List<Long> timeStamps){
    
    
    Connection connection = createOSMatrixDBConnection();
    try {
      
      Calendar local = Calendar.getInstance(TimeZone.getTimeZone("UTC"));    
      
      
      String query = "INSERT INTO times (time) VALUES (?)";
      PreparedStatement ps = connection.prepareStatement(query);            
      
      for (Long record : timeStamps) {
          Timestamp ts = new Timestamp(record*1000);
          ps.setTimestamp(1, ts, local);
          ps.addBatch();
      }
      ps.executeBatch();
      

    } catch (SQLException e) {
      // TODO Auto-generated catch block
      System.err.println("could not fill times table!");
      e.printStackTrace();
      
    }        
    
  }
    public void insertOSMatrixAttributeTypes(String attribute, String description, String title, Timestamp validFrom ){
    
    Connection connection = createOSMatrixDBConnection();
    try {
      
      Calendar local = Calendar.getInstance(TimeZone.getTimeZone("UTC"));    
            
      String query = "INSERT INTO attribute_types (attribute, description, title, validFrom) VALUES (?,?,?,?)";
      PreparedStatement ps = connection.prepareStatement(query);            
      

          ps.setString(1, attribute);
          ps.setString(2, description);
          ps.setString(3, title);
          ps.setTimestamp(4, validFrom, local);
          ps.addBatch();
 
      ps.executeBatch();
      

    } catch (SQLException e) {
      // TODO Auto-generated catch block
      System.err.println("could not instert into attributes_types table!");
      e.printStackTrace();
      
    }  
    
    }
    }
