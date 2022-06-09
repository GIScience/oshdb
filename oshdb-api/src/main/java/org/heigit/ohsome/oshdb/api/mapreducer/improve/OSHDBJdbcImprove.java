package org.heigit.ohsome.oshdb.api.mapreducer.improve;

import com.google.common.base.Joiner;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

/**
 * OSHDB database backend connector to a JDBC database file.
 */
public class OSHDBJdbcImprove extends OSHDBJdbc {


  public OSHDBJdbcImprove(String classToLoad, String jdbcString, String user, String pw)
      throws SQLException, ClassNotFoundException {
    super(classToLoad, jdbcString, user, pw);
  }

  public OSHDBJdbcImprove(Connection conn) {
    super(conn);
  }

  public MapReducer<OSHEntity> createMapReducerImprove(OSHDBView<?> view) {
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
    return new MapReducerJdbcImprove<>(this, x -> Stream.of(x), view);
  }

  private static class MapReducerJdbcImprove<X> extends MapReducerImprove<X> {

    private OSHDBJdbcImprove oshdb;
    private OSHDBView<?> view;

    public MapReducerJdbcImprove(OSHDBJdbcImprove oshdb,
        SerializableFunction<OSHEntity, Stream<X>> transform, OSHDBView<?> view) {
      super(transform);
      this.oshdb = oshdb;
      this.view = view;
    }

    @Override
    protected <R> MapReducer<R> of(SerializableFunction<OSHEntity, Stream<R>> fnt) {
      return new MapReducerJdbcImprove<>(oshdb, fnt, view);
    }

    @Override
    public <S> S reduce(SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
      return Streams.stream(view.getCellIdRanges())
          .flatMap(this::getOshCellsStream)
          .flatMap(grid -> Streams.stream(grid.getEntities()))
          .filter(view::preFilter)
          .map(osh ->  Stream.of(osh).flatMap(transform)
              .reduce(identitySupplier.get(), accumulator, combiner))
          .reduce(combiner).orElseGet(identitySupplier);
    }

    @Override
    public Stream<X> stream() {
      return Streams.stream(view.getCellIdRanges())
        .flatMap(this::getOshCellsStream)
        .flatMap(grid -> Streams.stream(grid.getEntities()))
        .filter(view::preFilter)
        .flatMap(osh ->  Stream.of(osh).flatMap(transform));
    }


    protected ResultSet getOshCellsRawDataFromDb(CellIdRange cellIdRange)
        throws SQLException {
      String sqlQuery = view.getTypeFilter().stream()
          .map(osmType ->
          TableNames.forOSMType(osmType).map(tn -> tn.toString(this.oshdb.prefix()))
              )
          .filter(Optional::isPresent).map(Optional::get)
          .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
          .collect(Collectors.joining(" union all "));
      PreparedStatement pstmt = ((OSHDBJdbc) this.oshdb).getConnection().prepareStatement(sqlQuery);
      pstmt.setInt(1, cellIdRange.getStart().getZoomLevel());
      pstmt.setLong(2, cellIdRange.getStart().getId());
      pstmt.setLong(3, cellIdRange.getEnd().getId());
      return pstmt.executeQuery();
    }

    /**
     * Returns data of one cell from the raw data stream.
     */
    protected GridOSHEntity readOshCellRawData(ResultSet oshCellsRawData)
        throws IOException, ClassNotFoundException, SQLException {
      return (GridOSHEntity)
          (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
    }

    @Nonnull
    protected Stream<GridOSHEntity> getOshCellsStream(CellIdRange cellIdRange) {
      try {
        if (view.getTypeFilter().isEmpty()) {
          return Stream.empty();
        }
        ResultSet oshCellsRawData = getOshCellsRawDataFromDb(cellIdRange);
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
                } catch (IOException | ClassNotFoundException | SQLException e) {
                  throw new OSHDBException(e);
                }
              }
            }, 0
            ), false);
      } catch (SQLException e) {
        throw new OSHDBException(e);
      }
    }
  }
}
