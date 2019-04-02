package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CancelableProcessStatus;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTWriter;

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

  protected ResultSet getOshCellsRawDataFromDb(CellIdRange cellIdRange)
      throws SQLException {
    String sqlQuery = this.typeFilter.stream()
        .map(osmType ->
            TableNames.forOSMType(osmType).map(tn -> tn.toString(this.oshdb.prefix()))
        )
        .filter(Optional::isPresent).map(Optional::get)
        .map(tn -> "(select data from " + tn + " where level = ?1 and id between ?2 and ?3)")
        .collect(Collectors.joining(" union all "));
    PreparedStatement pstmt = ((OSHDBJdbc)this.oshdb).getConnection().prepareStatement(sqlQuery);
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
  protected Stream<? extends GridOSHEntity> getOshCellsStream(CellIdRange cellIdRange) {
    try {
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
                throw new RuntimeException(e);
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
                throw new RuntimeException(e);
              }
            }
          }, 0
      ), false);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected ArrayList<GridOSHEntity> getUpdates()
      throws SQLException, IOException, ClassNotFoundException {
    WKTWriter wktWriter = new WKTWriter();
    String sqlQuery;
    if (!"org.apache.ignite.internal.jdbc.JdbcConnection".equals(this.update
        .getConnection().getClass().getName())) {
      sqlQuery = this.getPostGISQuery();
    } else {
      sqlQuery = this.getIgniteQuery();
    }
    PreparedStatement pstmt = this.update.getConnection().prepareStatement(sqlQuery);
    Polygon geometry = OSHDBGeometryBuilder.getGeometry(this.bboxFilter);
    pstmt.setObject(1, wktWriter.write(geometry));
    ResultSet updateEntities = pstmt.executeQuery();
    
    ArrayList<GridOSHEntity> result=new ArrayList<>(10);
    while (updateEntities.next()) {
      byte[] data = updateEntities.getBytes("data");
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      ObjectInputStream ois = new ObjectInputStream(bais);
      OSHEntity entity = (OSHEntity) ois.readObject();

      GridOSHEntity updateCell = null;
      switch (entity.getType()) {
        case NODE:
          ArrayList<OSHNode> nodes = new ArrayList<>(1);
          nodes.add((OSHNode) entity);
          updateCell = GridOSHNodes.rebase(0, 0, 0, 0, 0, 0, nodes);
          break;
        case WAY:
          ArrayList<OSHWay> ways = new ArrayList<>(1);
          ways.add((OSHWay) entity);
          updateCell = GridOSHWays.compact(0, 0, 0, 0, 0, 0, ways);
          break;
        case RELATION:
          ArrayList<OSHRelation> relations = new ArrayList<>(1);
          relations.add((OSHRelation) entity);
          updateCell = GridOSHRelations.compact(0, 0, 0, 0, 0, 0, relations);
          break;
        default:
          throw new AssertionError(entity.getType().name());
      }
      result.add(updateCell);
    }
    return result;
  }

  private String getIgniteQuery() {
    return this.typeFilter.stream()
        .map(osmType ->
            TableNames.forOSMType(osmType).map(tn -> tn.toString(this.oshdb.prefix()))
        )
        .filter(Optional::isPresent).map(Optional::get)
        .map(tn ->
            "(SELECT data FROM " + tn + " WHERE bbx && ?)")
        .collect(Collectors.joining(" union all "));
  }

  private String getPostGISQuery() {
    return this.typeFilter.stream()
        .map(osmType ->
            TableNames.forOSMType(osmType).map(tn -> tn.toString(this.oshdb.prefix()))
        )
        .filter(Optional::isPresent).map(Optional::get)
        .map(tn ->
            "(SELECT data FROM " + tn + " WHERE ST_Intersects(bbx,ST_GeomFromText(?,4326)))")
        .collect(Collectors.joining(" union all "));
  }

}
