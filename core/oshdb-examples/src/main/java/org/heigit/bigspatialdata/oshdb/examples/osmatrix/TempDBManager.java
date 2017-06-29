package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class TempDBManager {
  
  private static DataSource datasource;
  
//  CREATE TABLE attributes_temp
//  (
//    id serial NOT NULL,
//    cell_id integer NOT NULL,
//    attribute_type_id integer NOT NULL,
//    value double precision NOT NULL,
//    valid integer NOT NULL,
//  CONSTRAINT pk_attributes_temp PRIMARY KEY (id)
//    )
//    
//  WITH (
//    OIDS=FALSE
//  )
//  TABLESPACE osmatrix_data;
//  ALTER TABLE attributes_temp
//    OWNER TO osmatrix;
//
//  -- Index: attributes_id_index
//
//  -- DROP INDEX attributes_id_index;
//
//  CREATE INDEX attributes_temp_id_index
//    ON attributes_temp
//    USING btree
//    (id)
//  TABLESPACE osmatrix_index;
  
  public static DataSource getDataSource(String connString, String user, String password)
  {
      if(datasource == null)
      {
          HikariConfig config = new HikariConfig();
          
          config.setJdbcUrl(connString);
          config.setUsername(user);
          config.setPassword(password);

//          config.setMaximumPoolSize(10);
//          config.setAutoCommit(false);
//          config.addDataSourceProperty("cachePrepStmts", "true");
//          config.addDataSourceProperty("prepStmtCacheSize", "250");
//          config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
          
          datasource = new HikariDataSource(config);
      }
      return datasource;
  }
  
//  public static void main(String[] args)
//  {
//            
//      Connection connection = null;
//      PreparedStatement pstmt = null;
//      ResultSet resultSet = null;
//      try
//      {
//          DataSource dataSource = osmatrixTempDBManager.getDataSource("jdbc:postgresql://lemberg.geog.uni-heidelberg.de:5432/osmatrixhd","osmatrix", "osmatrix2016");
//          connection = dataSource.getConnection();
//          pstmt = connection.prepareStatement("SELECT * FROM attribute_types");
//          
//          System.out.println("The Connection Object is of Class: " + connection.getClass());
//          
//          resultSet = pstmt.executeQuery();
//          while (resultSet.next())
//          {
//              System.out.println(resultSet.getString(1) + "," + resultSet.getString(2) + "," + resultSet.getString(3));
//          }
//
//      }
//      catch (Exception e)
//      {
//          try
//          {
//              connection.rollback();
//          }
//          catch (SQLException e1)
//          {
//              e1.printStackTrace();
//          }
//          e.printStackTrace();
//      }
//      
//  }

}
