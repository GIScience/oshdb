package org.heigit.ohsome.oshdb.api.db;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerJdbcMultithread;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerJdbcSinglethread;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.util.tagtranslator.JdbcTagTranslator;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;

/**
 * OSHDB database backend connector to a JDBC database file.
 */
public class OSHDBJdbc extends OSHDBDatabase {

  protected final DataSource dataSource;
  protected final DataSource keytablesSource;
  protected JdbcTagTranslator tagTranslator;
  private boolean useMultithreading = true;

  public OSHDBJdbc(DataSource source) {
    this(source, source);
  }

  public OSHDBJdbc(DataSource source, DataSource keytables) {
    this.dataSource = source;
    this.keytablesSource = keytables;
  }

  @Override
  public TagTranslator getTagTranslator() {
    if (tagTranslator == null) {
      tagTranslator = new JdbcTagTranslator(keytablesSource);
    }
    return tagTranslator;
  }

  @Override
  public OSHDBJdbc prefix(String prefix) {
    return (OSHDBJdbc) super.prefix(prefix);
  }

  @Override
  public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    try {
      Collection<String> expectedTables = Stream.of(OSMType.values()).map(TableNames::forOSMType)
          .filter(Optional::isPresent).map(Optional::get)
          .map(t -> t.toString(this.prefix()).toLowerCase()).collect(Collectors.toList());
      List<String> allTables = new LinkedList<>();
      try (var conn = getConnection()) {
        var metaData = conn.getMetaData();
        try (var rs = metaData.getTables(null, null, "%", new String[] {"TABLE"})) {
          while (rs.next()) {
            allTables.add(rs.getString("TABLE_NAME").toLowerCase());
          }
          if (!allTables.containsAll(expectedTables)) {
            throw new OSHDBTableNotFoundException(Joiner.on(", ").join(expectedTables));
          }
        }
      }
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
    MapReducer<X> mapReducer;
    if (this.useMultithreading) {
      mapReducer = new MapReducerJdbcMultithread<>(this, forClass);
    } else {
      mapReducer = new MapReducerJdbcSinglethread<>(this, forClass);
    }
    return mapReducer;
  }

  @Override
  public String metadata(String property) {
    var table = TableNames.T_METADATA.toString(this.prefix());
    var selectSql = String.format("select value from %s where key=?", table);
    try (var conn = getConnection();
         var stmt = conn.prepareStatement(selectSql)) {
      stmt.setString(1, property);
      try (var result = stmt.executeQuery()) {
        if (result.next()) {
          return result.getString(1);
        }
        return null;
      }
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
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
  }
}
