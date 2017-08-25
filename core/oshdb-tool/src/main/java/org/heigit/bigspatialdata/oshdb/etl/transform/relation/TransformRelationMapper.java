package org.heigit.bigspatialdata.oshdb.etl.transform.relation;

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
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import org.heigit.bigspatialdata.oshdb.etl.TableNames;
import org.heigit.bigspatialdata.oshdb.etl.transform.TransformMapper2;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.CellRelation;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.NodeRelation;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.WayRelation;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSMType;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformRelationMapper extends TransformMapper2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformRelationMapper.class);

  public static class Result {

    private final SortedSet<CellRelation> cells;

    public Result(final SortedSet<CellRelation> cells) {
      this.cells = cells;
    }

    public Result() {
      cells = Collections.emptySortedSet();
    }

    public SortedSet<CellRelation> getCells() {
      return cells;
    }
  }

  private static final Result EMPTY_RESULT = new Result();

  private PreparedStatement pstmtRelationForRelation;

  private Map<Integer, Map<Long, CellRelation>> mapCellRelations;
  private final Map<Integer, XYGrid> gridHierarchy = new HashMap<>();

  private Map<Long, NodeRelation> nodeCache = new HashMap<>();
  private long lastNode = 0;

  private Map<Long, WayRelation> wayCache = new HashMap<>();
  private long lastWay = 0;

  private final int maxZoom;
  private final String nodeRelationFile;
  private final String wayRelationFile;
  private final Connection relationsdbconn;
  private final Connection keytables;

  public TransformRelationMapper(final int maxZoom, final String nodeRelationFile, final String wayRelationFile, Connection oshdbRelations, Connection keytables) {
    this.maxZoom = maxZoom;
    this.nodeRelationFile = nodeRelationFile;
    this.wayRelationFile = wayRelationFile;
    this.relationsdbconn = oshdbRelations;
    this.keytables = keytables;

    for (int i = 1; i <= maxZoom; i++) {
      gridHierarchy.put(i, new XYGrid(i));
    }

  }

  public Result map(InputStream in) {
    try (//
            Connection connKeyTables = this.keytables;
            Connection connRelations = this.relationsdbconn) {

      initKeyTables(connKeyTables);

      pstmtRelationForRelation = connRelations
              .prepareStatement("select relations from " + TableNames.E_RELATION2RELATION.toString() + " where relation = ?");

      try ( //
              final FileInputStream fileNodeStream = new FileInputStream(nodeRelationFile);
              final BufferedInputStream bufferedNodeStream = new BufferedInputStream(fileNodeStream);
              final ObjectInputStream relationNodeStream = new ObjectInputStream(bufferedNodeStream);
              final FileInputStream fileWayStream = new FileInputStream(wayRelationFile);
              final BufferedInputStream bufferedWayStream = new BufferedInputStream(fileWayStream);
              final ObjectInputStream relationWayStream = new ObjectInputStream(bufferedWayStream);
              final OsmPrimitiveBlockIterator blockItr = new OsmPrimitiveBlockIterator(in)) {

        final OsmPbfIterator osmIterator = new OsmPbfIterator(blockItr);
        final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

        this.mapCellRelations = new HashMap<>();

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
            LOGGER.warn("emyty list of versions!");
            continue;
          }
          final long id = versions.get(0).getId();
          final Type type = versions.get(0).getType();

          if (type != Type.RELATION) {
            continue;
          }

          long minTimestamp = Long.MAX_VALUE;
          SortedSet<Long> nodes = new TreeSet<>();
          SortedSet<Long> ways = new TreeSet<>();
          List<OSMRelation> relations = new ArrayList<>(versions.size());
          for (OSMPbfEntity entity : versions) {
            OSMPbfRelation pbfRelation = (OSMPbfRelation) entity;
            relations.add(getRelation(pbfRelation));
            minTimestamp = Math.min(minTimestamp, pbfRelation.getTimestamp());

            for (OSMPbfRelation.OSMMember member : pbfRelation.getMembers()) {
              try {
                switch (OSMType.fromInt(member.getType())) {
                  case NODE: {
                    nodes.add(member.getMemId());
                    break;
                  }
                  case WAY: {
                    ways.add(member.getMemId());
                    break;
                  }
                  default: {
                    break;
                  }
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }

          List<OSHNode> oshNodes = getNodes(relationNodeStream, id, nodes);
          List<OSHWay> oshWays = getWays(relationWayStream, id, ways);

          OSHRelation oshRelation = OSHRelation.build(relations, oshNodes, oshWays);

          CellRelation cell = getCell(oshNodes, oshWays);
          cell.add(oshRelation, minTimestamp);
        } // while blockItr.hasNext();

        final SortedSet<CellRelation> cellOutput = new TreeSet<>();
        for (Map<Long, CellRelation> level : mapCellRelations.values()) {
          for (CellRelation cell : level.values()) {
            if (!cell.getRelations().isEmpty()) {
              cellOutput.add(cell);
            }
          }
        }

        return new Result(cellOutput);

      }
    } catch (ClassNotFoundException | SQLException | IOException e) {
      e.printStackTrace();
    }

    return EMPTY_RESULT;
  }

  private CellRelation getCell(List<OSHNode> nodes, List<OSHWay> ways) {
    double minLon = Double.MAX_VALUE;
    double maxLon = Double.MIN_VALUE;
    double minLat = Double.MAX_VALUE;
    double maxLat = Double.MIN_VALUE;

    for (OSHNode osh : nodes) {
      Iterator<OSMNode> osmItr = osh.iterator();
      while (osmItr.hasNext()) {
        OSMNode osm = osmItr.next();
        if (osm.isVisible()) {
          minLon = Math.min(minLon, osm.getLongitude());
          maxLon = Math.max(maxLon, osm.getLongitude());
          minLat = Math.min(minLat, osm.getLatitude());
          maxLat = Math.max(maxLat, osm.getLatitude());
        }
      }
    }

    for (OSHWay osh : ways) {
      BoundingBox bbox = osh.getBoundingBox();
      minLon = Math.min(minLon, bbox.minLon);
      maxLon = Math.max(maxLon, bbox.maxLon);

      minLat = Math.min(minLat, bbox.minLat);
      maxLat = Math.max(maxLat, bbox.maxLat);
    }

    int level;
    long cellId;
    if (minLon > maxLon || minLat > maxLat) {
      level = 0;
      cellId = -1;

    } else {
      final NumericRange lonRange = new NumericRange(minLon, maxLon);
      final NumericRange latRange = new NumericRange(minLat, maxLat);
      final BasicNumericDataset boundingBox = new BasicNumericDataset(new NumericData[]{lonRange, latRange});

      level = maxZoom;
      XYGrid grid = null;
      for (; level > 0; level--) {
        grid = gridHierarchy.get(level);
        if (grid.getEstimatedIdCount(boundingBox) <= 4) {
          break;
        }
      }
      cellId = grid.getId(lonRange, latRange);
    }

    Map<Long, CellRelation> levelCell = mapCellRelations.get(level);
    if (levelCell == null) {
      levelCell = new HashMap<>();
      mapCellRelations.put(level, levelCell);
    }

    CellRelation cell = levelCell.get(cellId);
    if (cell == null) {
      cell = new CellRelation(cellId, level);
      levelCell.put(cellId, cell);
    }

    return cell;
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

  private List<OSHWay> getWays(final ObjectInputStream relationStream, final long id, SortedSet<Long> refs)
          throws IOException, ClassNotFoundException {
    List<OSHWay> ways = new ArrayList<>(refs.size());

    for (Long refId : refs) {
      if (Long.compare(refId.longValue(), lastWay) <= 0) {
        WayRelation nr = wayCache.get(refId);
        if (nr != null) {
          ways.add(nr.way());
          if (nr.getMaxRelationId() <= id) {
            wayCache.remove(refId);
          }
        }
      } else {
        try {
          // node is not in cache, read it from inputstream
          WayRelation nr = (WayRelation) relationStream.readObject();
          while (nr.way().getId() < refId.longValue()) {
            // cache the noderelations
            wayCache.put(nr.way().getId(), nr);
            nr = (WayRelation) relationStream.readObject();
          }
          lastWay = nr.way().getId();

          if (nr.way().getId() == refId.longValue()) {
            ways.add(nr.way());
          }

          // has the noderelation further relation than
          // cache it
          if (nr.getMaxRelationId() > id) {
            wayCache.put(nr.way().getId(), nr);
          }

        } catch (EOFException e) {
          System.err.printf("missing RefId %d, lastRelation: %d\n", refId.longValue(), lastWay);
          break;
        }
      }
    }
    return ways;
  }

  private OSMRelation getRelation(OSMPbfRelation entity) {
    return new OSMRelation(entity.getId(), //
            entity.getVersion() * (entity.getVisible() ? 1 : -1), //
            entity.getTimestamp(), //
            entity.getChangeset(), //
            entity.getUser().getId(), //
            getKeyValue(entity.getTags()), //
            convertMembers(entity.getMembers()));
  }

}
