package org.heigit.ohsome.oshdb.api.db;

import static java.util.stream.Collectors.toList;

import com.google.common.base.Joiner;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerJdbcMultithread;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.MapReducerJdbcSinglethread;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;

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
          .collect(toList());
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
      mapReducer = new MapReducerJdbcMultithread<>(this, forClass);
    } else {
      mapReducer = new MapReducerJdbcSinglethread<>(this, forClass);
    }
    mapReducer = mapReducer.keytables(this);
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

  static class Collector<T, R> {
    private final BiFunction<R, T, R> accumulator;
    private final BinaryOperator<R> combiner;

    private R r;

    Collector(R r, BiFunction<R, T, R> accumulator, BinaryOperator<R> combiner) {
      this.r = r;
      this.accumulator = accumulator;
      this.combiner = combiner;
    }

    public void accumulate(T t) {
      r = accumulator.apply(r, t);
    }

    public Collector<T, R> combine(Collector<T, R> other) {
      r = combiner.apply(r, other.r);
      return this;
    }

    public R get() {
      return r;
    }
  }

  @Override
  public <X, Y> Y query(OSHDBView<?> view, SerializableFunction<OSHEntity, X> transform,
      Y identity, BiFunction<Y, X, Y> accumulator, BinaryOperator<Y> combiner) {
    var filter = view.getPreFilter();
    return Streams.stream(view.getCellIdRanges())
        .flatMap(range -> getOshCellsStream(range, view))
        .map(GridOSHEntity::getEntities)
        .flatMap(Streams::stream)
        .parallel()
          .map(OSHEntity.class::cast)
          .filter(filter)
          .map(transform)
        .collect(() -> new Collector<>(identity, accumulator, combiner),
            Collector::accumulate,
            Collector::combine)
         .get();
  }

  @Override
  public <X> Stream<X> query(OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<X>> transform) {
    var filter = view.getPreFilter();
    return Streams.stream(view.getCellIdRanges())
        .flatMap(range -> getOshCellsStream(range, view))
        .map(GridOSHEntity::getEntities)
        .flatMap(Streams::stream)
        .parallel()
        .map(OSHEntity.class::cast)
        .filter(filter)
        .flatMap(transform);
  }

  protected Stream<GridOSHEntity> getOshCellsStream(CellIdRange cellIdRange, OSHDBView<?> view) {
    try {
      if (view.getTypeFilter().isEmpty()) {
        return Stream.empty();
      }
      var oshCellsRawData = getOshCellsRawDataFromDb(cellIdRange, view);
      if (!oshCellsRawData.next()) {
        return Stream.empty();
      }
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
          new Iterator<GridOSHEntity>() {
            @Override
            public boolean hasNext() {
              try {
                return !oshCellsRawData.isClosed();
              } catch (SQLException e) {
                throw new OSHDBException(e);
              }
            }

            @Override
            public GridOSHEntity next() {
              try {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                GridOSHEntity data = readOshCellRawData(oshCellsRawData);
                if (!oshCellsRawData.next()) {
                  oshCellsRawData.close();
                }
                return data;
              } catch (Exception e) {
                throw new OSHDBException(e);
              }
            }
          }, 0
          ), false).onClose(() -> {try { oshCellsRawData.close(); } catch (SQLException e) {}});
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }

  protected ResultSet getOshCellsRawDataFromDb(CellIdRange cellIdRange, OSHDBView<?> view)
      throws SQLException {
    String sqlQuery = view.getTypeFilter().stream()
        .map(osmType ->
        TableNames.forOSMType(osmType).map(tn -> tn.toString(prefix()))
            )
        .filter(Optional::isPresent).map(Optional::get)
        .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
        .collect(Collectors.joining(" union all "));
    PreparedStatement pstmt = getConnection().prepareStatement(sqlQuery);
    pstmt.setInt(1, cellIdRange.getStart().getZoomLevel());
    pstmt.setLong(2, cellIdRange.getStart().getId());
    pstmt.setLong(3, cellIdRange.getEnd().getId());
    return pstmt.executeQuery();
  }

  protected GridOSHEntity readOshCellRawData(ResultSet oshCellsRawData)
      throws IOException, ClassNotFoundException, SQLException {
    return (GridOSHEntity)
        (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
  }
}
