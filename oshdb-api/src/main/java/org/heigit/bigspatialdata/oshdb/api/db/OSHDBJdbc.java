package org.heigit.bigspatialdata.oshdb.api.db;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerJdbcMultithread;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerJdbcSinglethread;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTableNotFoundException;

/**
 * OSHDB database backend connector to a JDBC database file.
 */
public class OSHDBJdbc extends OSHDBDatabase implements AutoCloseable {

  protected Connection connection;
  private boolean useMultithreading = true;

  public OSHDBJdbc(String classToLoad, String jdbcString)
      throws SQLException, ClassNotFoundException {
    this(classToLoad, jdbcString, "sa", "");
  }

  public OSHDBJdbc(String classToLoad, String jdbcString, String user, String pw)
      throws SQLException, ClassNotFoundException {
    Class.forName(classToLoad);
    this.connection = DriverManager.getConnection(jdbcString, user, pw);
  }

  public OSHDBJdbc(Connection conn) {
    this.connection = conn;
  }

  @Override
  public OSHDBJdbc prefix(String prefix) {
    return (OSHDBJdbc) super.prefix(prefix);
  }

  @Override
  public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    try {
      Collection<String> expectedTables = Stream.of(OSMType.values())
          .map(TableNames::forOSMType).filter(Optional::isPresent).map(Optional::get)
          .map(t -> t.toString(this.prefix()).toLowerCase())
          .collect(Collectors.toList());
      List<String> allTables = new LinkedList<>();
      ResultSet rs = this.getConnection().getMetaData().getTables(null, null,
          "%", new String[]{"TABLE"}
      );
      while (rs.next()) {
        allTables.add(rs.getString("TABLE_NAME").toLowerCase());
      }
      if (!allTables.containsAll(expectedTables)) {
        throw new OSHDBTableNotFoundException(Joiner.on(", ").join(expectedTables));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    MapReducer<X> mapReducer;
    if (this.useMultithreading) {
      mapReducer = new MapReducerJdbcMultithread<X>(this, forClass);
    } else {
      mapReducer = new MapReducerJdbcSinglethread<X>(this, forClass);
    }
    mapReducer = mapReducer.keytables(this);
    return mapReducer;
  }

  @Override
  public String metadata(String property) {
    try {
      PreparedStatement stmt = connection.prepareStatement(
          "SELECT value from " + TableNames.T_METADATA.toString(this.prefix()) + " where key=?"
      );
      stmt.setString(1, property);
      ResultSet result = stmt.executeQuery();
      if (result.next()) {
        return result.getString(1);
      } else {
        return null;
      }
    } catch (SQLException ignored) {
      return null;
    }
  }

  public Connection getConnection() {
    return this.connection;
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
    this.connection.close();
  }
}
