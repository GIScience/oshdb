package org.heigit.ohsome.oshdb.api.db;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;

/**
 * OSHDB database backend connector to a JDBC database file.
 */
public class OSHDBJdbcImprove extends OSHDBJdbc {


  public OSHDBJdbcImprove(String path) throws ClassNotFoundException, SQLException {
    super("org.h2.Driver", "jdbc:h2:" + path + ";ACCESS_MODE_DATA=r", "sa", "");
  }

  public OSHDBJdbcImprove(String classToLoad, String jdbcString, String user, String pw)
      throws SQLException, ClassNotFoundException {
    super(classToLoad, jdbcString, user, pw);
  }

  public OSHDBJdbcImprove(Connection conn) {
    super(conn);
  }

  @Override
  public <X> Stream<X> query(OSHDBView<?> view,
      SerializableFunction<Stream<OSHEntity>, Stream<X>> transform) {
    var filter = view.getPreFilter();
    return Streams.stream(view.getCellIdRanges())
      .parallel()
        .flatMap(range -> getOshCellsStream(range, view))
        .map(GridOSHEntity::getEntities)
        .map(Streams::stream)
        .map(stream -> stream.map(OSHEntity.class::cast))
        .map(stream -> stream.filter(filter))
        .map(transform)
        .map(stream -> stream.collect(toList()))
      .sequential()
      .flatMap(Collection::stream);
  }

  @Override
  public <X, Y> Y query(OSHDBView<?> view,
      SerializableFunction<Stream<OSHEntity>, X> transform,
      Y identity,
      BiFunction<Y, X, Y> accumulator,
      BinaryOperator<Y> combiner) {
    var filter = view.getPreFilter();
    return Streams.stream(view.getCellIdRanges())
      .parallel()
        .flatMap(range -> getOshCellsStream(range, view))
        .map(GridOSHEntity::getEntities)
        .map(Streams::stream)
        .map(stream -> stream.map(OSHEntity.class::cast))
        .map(stream -> stream.filter(filter))
        .map(transform)
      .sequential()
      .reduce(identity, accumulator, combiner);
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

  /**
   * Returns data of one cell from the raw data stream.
   */
  protected GridOSHEntity readOshCellRawData(ResultSet oshCellsRawData)
      throws IOException, ClassNotFoundException, SQLException {
    return (GridOSHEntity)
        (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
  }

  @Nonnull
  protected Stream<GridOSHEntity> getOshCellsStream(CellIdRange cellIdRange, OSHDBView view) {
    try {
      if (view.getTypeFilter().isEmpty()) {
        return Stream.empty();
      }
      ResultSet oshCellsRawData = getOshCellsRawDataFromDb(cellIdRange, view);
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
