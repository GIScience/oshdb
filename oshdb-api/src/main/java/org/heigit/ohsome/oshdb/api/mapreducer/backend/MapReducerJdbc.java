package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.Kernels.CancelableProcessStatus;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;

abstract class MapReducerJdbc<X> extends MapReducer<X> implements CancelableProcessStatus {

  /**
   * Stores the start time of reduce/stream operation as returned by
   * {@link System#currentTimeMillis()}. Used to determine query timeouts.
   */
  protected long executionStartTimeMillis;

  MapReducerJdbc(OSHDBDatabase oshdb, Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  MapReducerJdbc(MapReducerJdbc<?> obj) {
    super(obj);
  }

  @Override
  public boolean isActive() {
    if (timeout != null && System.currentTimeMillis() - executionStartTimeMillis > timeout) {
      throw new OSHDBTimeoutException();
    }
    return true;
  }

  protected List<Map.Entry<String, CellId>> getOshTableCellIds(CellIdRange cellIdRange)
      throws SQLException {
    var cellIds = new ArrayList<Map.Entry<String, CellId>>();
    for (var type : typeFilter) {
      var table = TableNames.forOSMType(type).map(tn -> tn.toString(this.oshdb.prefix()));
      if (table.isPresent()) {
        var tableName = table.get();
        var sqlQuery = String.format(
            "(select level, id from %s where level = ?1 and id between ?2 and ?3)", tableName);
        try (var pstmt = ((OSHDBJdbc) this.oshdb).getConnection().prepareStatement(sqlQuery)) {
          pstmt.setInt(1, cellIdRange.getStart().getZoomLevel());
          pstmt.setLong(2, cellIdRange.getStart().getId());
          pstmt.setLong(3, cellIdRange.getEnd().getId());
          try (var rs = pstmt.executeQuery()) {
            while (rs.next()) {
              cellIds.add(Map.entry(tableName, new CellId(rs.getInt(1), rs.getLong(2))));
            }
          }
        }
      }
    }
    return cellIds;
  }

  /**
   * Returns data of one cell.
   */
  protected GridOSHEntity readOneGridCell(Map.Entry<String, CellId> entry) {
    var table = entry.getKey();
    var cellId = entry.getValue();
    var sqlQuery = String.format("select data from %s where level = ? and id = ?", table);
    try (var pstmt = ((OSHDBJdbc) this.oshdb).getConnection().prepareStatement(sqlQuery)) {
      pstmt.setInt(1, cellId.getZoomLevel());
      pstmt.setLong(2, cellId.getId());
      try (var rs = pstmt.executeQuery()) {
        if (!rs.next()) {
          throw new NoSuchElementException();
        }
        try (var bs = rs.getBinaryStream(1);
             var os = new ObjectInputStream(bs)) {
          return (GridOSHEntity) os.readObject();
        }
      }
    } catch (SQLException | IOException | ClassNotFoundException e) {
      throw new OSHDBException(e);
    }
  }

  @Nonnull
  protected Stream<GridOSHEntity> getOshCellsStream(CellIdRange cellIdRange) {
    try {
      if (this.typeFilter.isEmpty()) {
        return Stream.empty();
      }
      var oshCellIds = getOshTableCellIds(cellIdRange);
      if (oshCellIds.isEmpty()) {
        return Stream.empty();
      }
      return oshCellIds.stream().map(this::readOneGridCell);
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }
}
