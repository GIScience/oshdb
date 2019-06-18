package org.heigit.bigspatialdata.oshdb.api.object;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHWayImpl;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps around a ResultSet of updated Entities.
 */
public class UpdateIterator implements Iterator<GridOSHEntity> {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateIterator.class);

  private final List<OSHEntity> batch;
  private final int batchSize;
  private final OSHDBBoundingBox copyOfBboxFilter;
  private GridOSHEntity next;
  private final String prefix;
  private OSMType type;
  private final Iterator<OSMType> typeIt;
  private final Connection updateConn;
  private ResultSet updateEntities;

  /**
   * Iterator on an Update-ResultSet.
   *
   * @param typeIt "List" of types to be queried.
   * @param prefix prefix for tables
   * @param updateConn connection to update database
   * @param copyOfBboxFilter boundingbox for entities to get
   * @param batchSize number of entites to group in one GridCell
   */
  public UpdateIterator(Iterator<OSMType> typeIt,
      String prefix,
      Connection updateConn,
      OSHDBBoundingBox copyOfBboxFilter,
      int batchSize) {
    this.typeIt = typeIt;
    this.prefix = prefix;
    this.updateConn = updateConn;
    this.copyOfBboxFilter = copyOfBboxFilter;
    this.batchSize = batchSize;
    this.batch = new ArrayList<>(batchSize);
  }

  @Override
  public boolean hasNext() {
    try {
      return next != null || ((next = getNext()) != null);
    } catch (SQLException | IOException ex) {
      LOG.error("There was an error getting results. They will be incomplete.", ex);
    }
    if (updateEntities != null) {
      try {
        updateEntities.close();
      } catch (SQLException ex) {
      }
    }
    return false;
  }

  @Override
  public GridOSHEntity next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    GridOSHEntity returendEnt = next;
    next = null;
    return returendEnt;
  }

  @SuppressWarnings("unchecked")
  private GridOSHEntity createCell(OSMType type, List<? extends OSHEntity> elements)
      throws IOException {
    switch (type) {
      case NODE:
        return GridOSHNodes.rebase(0, 0, 0, 0, 0, 0, (List<OSHNode>) elements);
      case WAY:
        return GridOSHWays.compact(0, 0, 0, 0, 0, 0, (List<OSHWay>) elements);
      case RELATION:
        return GridOSHRelations.compact(0, 0, 0, 0, 0, 0, (List<OSHRelation>) elements);
      default:
        throw new AssertionError("type unknown: " + type);
    }
  }

  private String getIgniteQuery(OSMType type) {
    Optional<String> map = TableNames.forOSMType(type).map(tn -> tn.toString(prefix));
    if (map.isPresent()) {
      return "(SELECT data as data FROM  " + map.get() + " WHERE bbx && ?)";
    }
    return null;
  }

  private GridOSHEntity getNext() throws SQLException, IOException {
    WKTWriter wktWriter = new WKTWriter();
    if (updateEntities == null && typeIt.hasNext()) {
      type = typeIt.next();
      String sqlQuery;
      if (!"org.apache.ignite.internal.jdbc.JdbcConnection".equals(
          updateConn.getClass().getName()
      )) {
        sqlQuery = this.getPostGISQuery(type);
      } else {
        sqlQuery = this.getIgniteQuery(type);
      }
      PreparedStatement pstmt = updateConn.prepareStatement(sqlQuery);
      Polygon geometry = OSHDBGeometryBuilder.getGeometry(copyOfBboxFilter);
      pstmt.setObject(1, wktWriter.write(geometry));
      updateEntities = pstmt.executeQuery();
      updateEntities.setFetchSize(batchSize);
    }
    if (updateEntities == null) {
      return null;
    }
    boolean more;
    for (more = updateEntities.next(); more && batch.size() < batchSize; more = updateEntities
        .next()) {

      byte[] data = updateEntities.getBytes("data");
      switch (type) {
        case NODE:
          batch.add(OSHNodeImpl.instance(data, 0, data.length));

          break;

        case WAY:
          batch.add(OSHWayImpl.instance(data, 0, data.length));

          break;
        case RELATION:
          batch.add(OSHRelationImpl.instance(data, 0, data.length));

          break;
        default:
          throw new AssertionError("type unknown: " + type);
      }
    }
    if (!more) {
      try {
        updateEntities.close();
      } catch (SQLException ex) {
      }
      updateEntities = null;
    }
    if (batch.isEmpty()) {
      return null;
    }

    GridOSHEntity result = createCell(type, batch);
    batch.clear();
    return result;

  }

  private String getPostGISQuery(OSMType type) {
    Optional<String> map = TableNames.forOSMType(type)
        .map(tn -> tn.toString(prefix));
    if (map.isPresent()) {
      return "(SELECT data as data FROM " + map.get() + " WHERE ST_Intersects(bbx,ST_GeomFromText(?,4326)))";
    }
    return null;
  }

}
