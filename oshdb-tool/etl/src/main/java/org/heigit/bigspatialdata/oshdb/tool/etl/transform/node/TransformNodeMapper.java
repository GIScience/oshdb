package org.heigit.bigspatialdata.oshdb.tool.etl.transform.node;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.utils.dbaccess.TableNames;
import org.heigit.bigspatialdata.oshdb.tool.etl.transform.TransformMapper2;
import org.heigit.bigspatialdata.oshdb.tool.etl.transform.data.CellNode;
import org.heigit.bigspatialdata.oshdb.tool.etl.transform.data.NodeRelation;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformNodeMapper extends TransformMapper2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformNodeMapper.class);

  public static class Result {

    private final SortedSet<CellNode> nodeCells;
    private final List<NodeRelation> nodesForWays;
    private final List<NodeRelation> nodesForRelations;

    public Result(final SortedSet<CellNode> nodeCells, final List<NodeRelation> nodesForWays,
            final List<NodeRelation> nodesForRelations) {
      this.nodeCells = nodeCells;
      this.nodesForWays = nodesForWays;
      this.nodesForRelations = nodesForRelations;
    }

    public Result() {
      nodeCells = Collections.emptySortedSet();
      nodesForWays = Collections.emptyList();
      nodesForRelations = Collections.emptyList();
    }

    public SortedSet<CellNode> getNodeCells() {
      return nodeCells;
    }

    public List<NodeRelation> getNodesForWays() {
      return nodesForWays;
    }

    public List<NodeRelation> getNodesForRelations() {
      return nodesForRelations;
    }
  }

  private static final Result EMPTY_RESULT = new Result();

  private PreparedStatement getWaysForNode;
  private PreparedStatement getRelationsForNode;

  private Map<Long, CellNode> mapCellnodes;

  private final XYGrid grid;
  private final Connection oshdbconn;
  private final Connection keytables;

  public TransformNodeMapper(final int maxZoom, Connection oshdbRelations, Connection keytables) {
    this.grid = new XYGrid(maxZoom);
    this.oshdbconn = oshdbRelations;
    this.keytables = keytables;
  }

  public Result map(InputStream in) throws SQLException {
    Connection connKeyTables = this.keytables;
    Connection connRelations = this.oshdbconn;

    initKeyTables(connKeyTables);

    getWaysForNode = connRelations.prepareStatement("select ways from " + TableNames.E_NODE2WAY.toString() + " where node = ?");
    getRelationsForNode
            = connRelations.prepareStatement("select relations from " + TableNames.E_NODE2RELATION.toString() + " where node = ?");

    final List<NodeRelation> nodesForWays = new ArrayList<>();
    final List<NodeRelation> nodesForRelations = new ArrayList<>();

    mapCellnodes = new HashMap<>();

    try ( //
            final OsmPrimitiveBlockIterator blockItr = new OsmPrimitiveBlockIterator(in)) {

      final OsmPbfIterator osmIterator = new OsmPbfIterator(blockItr);
      final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

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

        if (type != Type.NODE) {
          break;
        }

        long minTimestamp = Long.MAX_VALUE;
        Set<CellNode> cells = new HashSet<>();
        List<OSMNode> nodes = new ArrayList<>(versions.size());
        for (OSMPbfEntity entity : versions) {
          OSMPbfNode pbfNode = (OSMPbfNode) entity;
          nodes.add(getNode(pbfNode));

          minTimestamp = Math.min(minTimestamp, pbfNode.getTimestamp());
          if (pbfNode.getVisible()) {
            cells.add(getCell(pbfNode.getLongitude(), pbfNode.getLatitude()));
          }
        }
        OSHNode hnode = OSHNode.build(nodes);

        for (CellNode cell : cells) {
          if (hnode.hasTags()) {
            cell.add(hnode, minTimestamp);
          }
        }

        Object[] ways = getRelation(getWaysForNode, hnode.getId());
        Object[] relation = getRelation(getRelationsForNode, hnode.getId());

        if (ways.length > 0) {
          nodesForWays.add(new NodeRelation(((Long) ways[ways.length - 1]).longValue(), hnode));
        }
        if (relation.length > 0) {
          nodesForRelations.add(new NodeRelation(((Long) relation[relation.length - 1]).longValue(), hnode));
        }
      } // while blockItr.hasNext();

      final SortedSet<CellNode> cellNodeOutput = new TreeSet<>();
      for (CellNode cellNode : mapCellnodes.values()) {
        if (!cellNode.getNodes().isEmpty()) {
          cellNodeOutput.add(cellNode);
        }
      }

      return new Result(cellNodeOutput, nodesForWays, nodesForRelations);

    } catch (IOException e) {
      e.printStackTrace();
    }

    return EMPTY_RESULT;
  }

  private OSMNode getNode(OSMPbfNode entity) {
    return new OSMNode(entity.getId(), //
            entity.getVersion() * (entity.getVisible() ? 1 : -1), //
            entity.getTimestamp(), //
            entity.getChangeset(), //
            entity.getUser().getId(), //
            getKeyValue(entity.getTags()), //
            entity.getLongitude(), entity.getLatitude());
  }

  private CellNode getCell(long longitude, long latitude) {

    long cellId = grid.getId(longitude * OSHDB.GEOM_PRECISION, latitude * OSHDB.GEOM_PRECISION);

    CellNode cellNode = mapCellnodes.get(cellId);
    if (cellNode == null) {
      cellNode = new CellNode(cellId, grid.getLevel());
      mapCellnodes.put(cellId, cellNode);
    }
    return cellNode;
  }

}
