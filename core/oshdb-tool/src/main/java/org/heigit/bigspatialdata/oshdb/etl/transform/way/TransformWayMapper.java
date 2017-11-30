package org.heigit.bigspatialdata.oshdb.etl.transform.way;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.etl.transform.TransformMapper2;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.CellWay;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.NodeRelation;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.WayRelation;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfWay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformWayMapper extends TransformMapper2 {

  private static final Logger LOG = LoggerFactory.getLogger(TransformWayMapper.class);

  private static final Result EMPTY_RESULT = new Result();

  private PreparedStatement pstmtRelationForWay;

  private Map<Integer, Map<Long, CellWay>> mapCellWays;
  private final Map<Integer, XYGrid> gridHierarchy = new HashMap<>();

  private Map<Long, NodeRelation> nodeCache = new HashMap<>();
  private long lastNode = 0;

  private final int maxZoom;
  private final String nodeRelationFile;
  private final Connection oshdbconn;
  private final Connection keytables;

  public TransformWayMapper(final int maxZoom, final String nodeRelationFile, Connection oshdbRelations, Connection keytables) {
    this.maxZoom = maxZoom;
    this.oshdbconn = oshdbRelations;
    this.keytables = keytables;
    this.nodeRelationFile = nodeRelationFile;
    for (int i = 1; i <= maxZoom; i++) {
      gridHierarchy.put(i, new XYGrid(i));
    }

  }

  public Result map(InputStream in) throws SQLException {
    Connection connKeyTables = this.keytables;
    Connection connRelations = this.oshdbconn;

    initKeyTables(connKeyTables);

    pstmtRelationForWay = connRelations.prepareStatement("select relations from " + TableNames.E_WAY2RELATION.toString() + " where way = ?");

    final List<WayRelation> waysForRelations = new ArrayList<>();

    try ( //
            final FileInputStream fileStream = new FileInputStream(nodeRelationFile);
            final BufferedInputStream bufferedStream = new BufferedInputStream(fileStream);
            final ObjectInputStream relationStream = new ObjectInputStream(bufferedStream);
            final OsmPrimitiveBlockIterator blockItr = new OsmPrimitiveBlockIterator(in)) {

      final OsmPbfIterator osmIterator = new OsmPbfIterator(blockItr);
      final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

      this.mapCellWays = new HashMap<>();

      clearKeyValueCache();

      Statement stmt = connKeyTables.createStatement();
      try (ResultSet rst = stmt.executeQuery(""
              + //
              "select " + TableNames.E_KEY.toString() + ".txt, " + TableNames.E_KEYVALUE.toString() + ".txt, keyid, valueid "
              + //
              "from " + TableNames.E_KEY.toString() + " join " + TableNames.E_KEYVALUE.toString() + " on " + TableNames.E_KEY.toString() + ".id = " + TableNames.E_KEYVALUE.toString() + ".keyid ")) {
        while (rst.next()) {
          Map<String, int[]> a = keyValueCache.get(rst.getString(1));
          if (a == null) {
            a = new HashMap<>();
            keyValueCache.put(rst.getString(1), a);
          }
          a.put(rst.getString(2), new int[]{rst.getInt(3), rst.getInt(4)});
        }
      }

      while (oshIterator.hasNext()) {
        final List<OSMPbfEntity> versions = oshIterator.next();
        if (versions.isEmpty()) {
          LOG.warn("emyty list of versions!");
          continue;
        }
        final long id = versions.get(0).getId();
        final Type type = versions.get(0).getType();

        if (type == Type.RELATION) {
          break;
        }

        if (type != Type.WAY) {
          continue;
        }

        long minTimestamp = Long.MAX_VALUE;
        SortedSet<Long> refs = new TreeSet<>();
        List<OSMWay> ways = new ArrayList<>(versions.size());
        for (OSMPbfEntity entity : versions) {
          OSMPbfWay pbfWay = (OSMPbfWay) entity;
          ways.add(getWay(pbfWay));
          minTimestamp = Math.min(minTimestamp, pbfWay.getTimestamp());
          refs.addAll(pbfWay.getRefs());
        }

        List<OSHNode> nodes = getNodes(relationStream, id, refs);

        OSHWay hway = OSHWay.build(ways, nodes);

        // Finde according cell!
        CellWay cell = getCell(nodes);

        cell.add(hway, minTimestamp);

        Object[] relation = getRelation(pstmtRelationForWay, hway.getId());

        if (relation.length != 0) {
          WayRelation r = new WayRelation(((Long) relation[relation.length - 1]).longValue(), hway);
          waysForRelations.add(r);
        }
      } // while blockItr.hasNext();

      final SortedSet<CellWay> cellOutput = new TreeSet<>();
      for (Map<Long, CellWay> level : mapCellWays.values()) {
        for (CellWay cell : level.values()) {
          if (!cell.getWays().isEmpty()) {
            cellOutput.add(cell);
          }
        }
      }
      return new Result(cellOutput, waysForRelations);

    } catch (IOException | ClassNotFoundException e) {
      LOG.error("Could not save map!", e);
    }

    return EMPTY_RESULT;
  }

  private List<OSHNode> getNodes(final ObjectInputStream relationStream, final long id, SortedSet<Long> refs)
          throws IOException, ClassNotFoundException {
    List<OSHNode> nodes = new ArrayList<>(refs.size());

    for (Long refId : refs) {
      if (Long.compare(refId.longValue(), lastNode) <= 0) {
        NodeRelation nr = nodeCache.get(refId);
        if (nr != null) {
          nodes.add(nr.node());
          if (nr.getMaxRelationId() <= id) {
            nodeCache.remove(refId);
          }
        }
      } else {
        // node is not in cache, read it from inputstream
        try {
          NodeRelation nr = (NodeRelation) relationStream.readObject();
          while (nr.node().getId() < refId.longValue()) {
            // cache the noderelations
            nodeCache.put(nr.node().getId(), nr);
            nr = (NodeRelation) relationStream.readObject();
          }
          lastNode = nr.node().getId();

          if (nr.node().getId() == refId.longValue()) {
            nodes.add(nr.node());
            // has the noderelation further relation than
            // cache it

          }

          if (nr.getMaxRelationId() > id) {
            nodeCache.put(nr.node().getId(), nr);
          }

        } catch (EOFException e) {
          System.err.printf("missing RefId %d, lastRelation: %d\n", refId.longValue(), lastNode);
          break;
        }
      }
    }
    return nodes;
  }

  private CellWay getCell(List<OSHNode> nodes) {
    long minLon = Long.MAX_VALUE;
    long maxLon = Long.MIN_VALUE;
    long minLat = Long.MAX_VALUE;
    long maxLat = Long.MIN_VALUE;

    for (OSHNode osh : nodes) {
      Iterator<OSMNode> osmItr = osh.iterator();
      while (osmItr.hasNext()) {
        OSMNode osm = osmItr.next();
        if (osm.isVisible()) {
          minLon = Math.min(minLon, osm.getLon());
          maxLon = Math.max(maxLon, osm.getLon());

          minLat = Math.min(minLat, osm.getLat());
          maxLat = Math.max(maxLat, osm.getLat());
        }
      }
    }

    final BoundingBox boundingBox = new BoundingBox(minLon * OSHDB.GEOM_PRECISION, maxLon * OSHDB.GEOM_PRECISION, minLat * OSHDB.GEOM_PRECISION, maxLat * OSHDB.GEOM_PRECISION);

    long ids;
    int l = maxZoom;
    XYGrid grid = null;
    for (; l > 0; l--) {
      grid = gridHierarchy.get(l);
      ids = grid.getEstimatedIdCount(boundingBox);
      if (ids <= 4) {
        break;
      }
    }

    long cellId = grid.getId(boundingBox);

    Map<Long, CellWay> level = mapCellWays.get(l);
    if (level == null) {
      level = new HashMap<>();
      mapCellWays.put(l, level);
    }

    CellWay cell = level.get(cellId);
    if (cell == null) {
      cell = new CellWay(cellId, grid.getLevel());
      level.put(cellId, cell);
    }

    return cell;
  }

  private OSMWay getWay(OSMPbfWay entity) {
    return new OSMWay(entity.getId(), //
            entity.getVersion() * (entity.getVisible() ? 1 : -1), //
            entity.getTimestamp(), //
            entity.getChangeset(), //
            entity.getUser().getId(), //
            getKeyValue(entity.getTags()), //
            convertLongs(entity.getRefs()));
  }

  public static class Result {

    private final SortedSet<CellWay> cells;
    private final List<WayRelation> waysForRelations;

    public Result(final SortedSet<CellWay> cells, final List<WayRelation> waysForRelations) {
      this.cells = cells;
      this.waysForRelations = waysForRelations;
    }

    public Result() {
      cells = Collections.emptySortedSet();
      waysForRelations = Collections.emptyList();
    }

    public SortedSet<CellWay> getCells() {
      return cells;
    }

    public List<WayRelation> getWaysForRelations() {
      return waysForRelations;
    }
  }
}
