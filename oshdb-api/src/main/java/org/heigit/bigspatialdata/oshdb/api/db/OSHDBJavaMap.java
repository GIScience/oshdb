package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerJavaMap;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class OSHDBJavaMap extends OSHDBDatabase {

  Map<String,String> metaData = new HashMap<>();
  final String jdbcString;
  final String user;
  final String pw;
  public OSHDBJavaMap(String classToLoad, String jdbcString, String user, String pw) throws SQLException, ClassNotFoundException {
    this.jdbcString = jdbcString;
    this.user = user;
    this.pw = pw;
    Class.forName(classToLoad);
    
    //TODO load Metadata
    //TODO load keytables
  }
  
  @Override
  public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    
    final Map<OSMType,Map<Long,GridOSHEntity>> database = new HashMap<>();
    try(Connection conn = DriverManager.getConnection(jdbcString, user, pw)){
            
      database.put(OSMType.NODE, loadTable(conn,TableNames.T_NODES.toString()));
      database.put(OSMType.WAY, loadTable(conn,TableNames.T_WAYS.toString()));
      database.put(OSMType.RELATION, loadTable(conn,TableNames.T_RELATIONS.toString()));
            
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    return new MapReducerJavaMap<>(this, forClass, database);
  }
  
  private Map<Long,GridOSHEntity> loadTable(Connection conn, String table) throws SQLException, ClassNotFoundException, IOException{
    Map<Long,GridOSHEntity> grids = new HashMap<>();
    try(Statement stmt = conn.createStatement();
        ResultSet rst = stmt.executeQuery("select id, data from "+table)){
      
      while(rst.next()){
        Long id  = rst.getLong(1);
        GridOSHEntity grid = (GridOSHEntity) (new ObjectInputStream(rst.getBinaryStream(2))).readObject();
        grids.put(id, grid);
      }
      
    }
    
    return grids;
  }
  

  @Override
  public String metadata(String property) {
    return metaData.get(property);
  }

}
