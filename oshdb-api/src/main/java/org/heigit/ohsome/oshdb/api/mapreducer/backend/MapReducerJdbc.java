package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.Kernels.CancelableProcessStatus;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
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
  MapReducerJdbc(MapReducerJdbc obj) {
    super(obj);
  }

  @Override
  public boolean isActive() {
    if (timeout != null && System.currentTimeMillis() - executionStartTimeMillis > timeout) {
      throw new OSHDBTimeoutException();
    }
    return true;
  }

  /**
   * Returns data of one cell from the raw data stream.
   */
  protected GridOSHEntity readOshCellRawData(ResultSet oshCellsRawData)
      throws IOException, ClassNotFoundException, SQLException {
    return (GridOSHEntity)
        (new ObjectInputStream(oshCellsRawData.getBinaryStream(1))).readObject();
  }

  protected String sqlQuery() {
    return this.typeFilter.stream()
        .map(osmType -> TableNames.forOSMType(osmType)
                .map(tn -> tn.toString(this.oshdb.prefix())))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
        .collect(joining(" union all "));
  }

  @Nonnull
  protected Stream<GridOSHEntity> getOshCellsStream(CellIdRange cellIdRange) {
    try {
      if (this.typeFilter.isEmpty()) {
        return Stream.empty();
      }

      var conn = ((OSHDBJdbc) this.oshdb).getConnection();
      var pstmt = conn.prepareStatement(sqlQuery());
      pstmt.setInt(1, cellIdRange.getStart().getZoomLevel());
      pstmt.setLong(2, cellIdRange.getStart().getId());
      pstmt.setLong(3, cellIdRange.getEnd().getId());
      var oshCellsRawData = pstmt.executeQuery();
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
          new Iterator<GridOSHEntity>() {
            GridOSHEntity next;
            @Override
            public boolean hasNext() {
              return next != null || (next = getNext()) != null;
            }

            @Override
            public GridOSHEntity next() {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              var grid = next;
              next = null;
              return grid;
            }

            private GridOSHEntity getNext() {
              try {
                if (!oshCellsRawData.next()) {
                  try {
                    oshCellsRawData.close();
                  } finally {
                    conn.close();
                  }
                  return null;
                }
                return readOshCellRawData(oshCellsRawData);
              } catch (IOException | ClassNotFoundException | SQLException e) {
                var exception = new OSHDBException(e);
                try {
                  conn.close();
                } catch (Exception e2) {
                  exception.addSuppressed(e2);
                }
                throw exception;
              }
            }
          }, 0
      ), false);
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }
}
