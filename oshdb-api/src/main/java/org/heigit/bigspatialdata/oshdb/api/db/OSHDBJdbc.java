package org.heigit.bigspatialdata.oshdb.api.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerJdbcMultithread;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerJdbcSinglethread;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;

public class OSHDBJdbc extends OSHDBDatabase implements AutoCloseable {

  protected Connection _conn;
  private boolean useMultithreading = true;

  public OSHDBJdbc(String classToLoad, String jdbcString) throws SQLException, ClassNotFoundException {
    this(classToLoad, jdbcString, "sa", "");
  }

  public OSHDBJdbc(String classToLoad, String jdbcString, String user, String pw) throws SQLException, ClassNotFoundException {
    Class.forName(classToLoad);
    this._conn = DriverManager.getConnection(jdbcString, user, pw);
  }

  public OSHDBJdbc(Connection conn) throws SQLException, ClassNotFoundException {
    this._conn = conn;
  }

  @Override
  public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    MapReducer<X> mapReducer;
    if (this.useMultithreading)
      mapReducer = new MapReducerJdbcMultithread<X>(this, forClass);
    else
      mapReducer = new MapReducerJdbcSinglethread<X>(this, forClass);
    mapReducer = mapReducer.keytables(this);
    return mapReducer;
  }

  @Override
  public String metadata(String property) {
    try {
      PreparedStatement stmt = _conn.prepareStatement(
          "SELECT value from " + TableNames.T_METADATA.toString(this.prefix()) + " where key=?"
      );
      stmt.setString(1, property);
      ResultSet result = stmt.executeQuery();
      if (result.next()) {
        return result.getString(1);
      }
    } catch (SQLException ignored) {}
    return null;
  }

  public Connection getConnection() {
    return this._conn;
  }

  public OSHDBJdbc multithreading(boolean useMultithreading) {
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
