package org.heigit.ohsome.oshdb.api.db;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerJdbcMultithread;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerJdbcSinglethread;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;

/**
 * OSHDB database backend connector to a JDBC database file.
 */
public class OSHDBJdbc extends OSHDBDatabase {

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
  public <X> MapReducer<X> createMapReducer(OSHDBView<X> view) {
    try {
      Collection<String> expectedTables = Stream.of(OSMType.values())
          .map(TableNames::forOSMType).filter(Optional::isPresent).map(Optional::get)
          .map(t -> t.toString(this.prefix()).toLowerCase())
          .collect(Collectors.toList());
      List<String> allTables = new LinkedList<>();
      var metaData = getConnection().getMetaData();
      try (var rs = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
        while (rs.next()) {
          allTables.add(rs.getString("TABLE_NAME").toLowerCase());
        }
        if (!allTables.containsAll(expectedTables)) {
          throw new OSHDBTableNotFoundException(Joiner.on(", ").join(expectedTables));
        }
      }
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }

    MapReducer<X> mapReducer;
    if (this.useMultithreading) {
      mapReducer = new MapReducerJdbcMultithread<>(this, view);
    } else {
      mapReducer = new MapReducerJdbcSinglethread<>(this, view);
    }
    return mapReducer;
  }

  @Override
  public String metadata(String property) {
    var table = TableNames.T_METADATA.toString(this.prefix());
    var selectSql = String.format("select value from %s where key=?", table);
    try (PreparedStatement stmt = connection.prepareStatement(selectSql)) {
      stmt.setString(1, property);
      try (var result = stmt.executeQuery()) {
        if (result.next()) {
          return result.getString(1);
        }
      }
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
    return null;
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
