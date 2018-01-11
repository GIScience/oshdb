package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_JDBC_multithread;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_JDBC_singlethread;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDB_MapReducible;

public class OSHDB_JDBC extends OSHDB_Database implements AutoCloseable {

  private final Connection _conn;
  private boolean useMultithreading = true;

  public OSHDB_JDBC(String classToLoad, String jdbcString) throws SQLException, ClassNotFoundException {
    this(classToLoad, jdbcString, "sa", "");
  }

  public OSHDB_JDBC(String classToLoad, String jdbcString, String user, String pw) throws SQLException, ClassNotFoundException {
    Class.forName(classToLoad);
    this._conn = DriverManager.getConnection(jdbcString, user, pw);
  }

  public OSHDB_JDBC(Connection conn) throws SQLException, ClassNotFoundException {
    this._conn = conn;
  }

  @Override
  public <X extends OSHDB_MapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    MapReducer<X> mapReducer;
    if (this.useMultithreading)
      mapReducer = new MapReducer_JDBC_multithread<X>(this, forClass);
    else
      mapReducer = new MapReducer_JDBC_singlethread<X>(this, forClass);
    mapReducer = mapReducer.keytables(this);
    return mapReducer;
  }

  public Connection getConnection() {
    return this._conn;
  }

  public OSHDB_JDBC multithreading(boolean useMultithreading) {
    this.useMultithreading = useMultithreading;
    return this;
  }

  public boolean multithreading() {
    return this.useMultithreading;
  }

  @Override
  public void close() throws Exception {
    this._conn.close();
  }
}
