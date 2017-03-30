package org.heigit.bigspatialdata.hosmdb.etl.transform.node;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.heigit.bigspatialdata.hosmdb.etl.transform.TransformMapper2;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.CellInfo;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.CellNode;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.NodeRelation;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;
import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;
import org.heigit.bigspatialdata.hosmdb.util.XYGrid;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfNode;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfTag;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
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
  
  private Map<Long,CellNode> mapCellnodes;
  
  private final XYGrid grid;

  public TransformNodeMapper(final int maxZoom) {
    this.grid = new XYGrid(maxZoom);
  }

  public Result map(InputStream in) {
    try (//
        Connection connKeyTables = DriverManager.getConnection("jdbc:h2:./hosmdb_keytables", "sa", "");
        Connection connRelations = DriverManager.getConnection("jdbc:h2:./temp_relations", "sa", "")) {

      initKeyTables(connKeyTables);
      

      getWaysForNode = connRelations.prepareStatement("select ways from node2way where node = ?");
      getRelationsForNode =
          connRelations.prepareStatement("select relations from node2relation where node = ?");

      final List<NodeRelation> nodesForWays = new ArrayList<>();
      final List<NodeRelation> nodesForRelations = new ArrayList<>();
      
      mapCellnodes = new HashMap<>();
      
      try ( //
          final OsmPrimitiveBlockIterator blockItr = new OsmPrimitiveBlockIterator(in)) {

        final OsmPbfIterator osmIterator = new OsmPbfIterator(blockItr);
        final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

        
        while (oshIterator.hasNext()) {
          final List<OSMPbfEntity> versions = oshIterator.next();
          if (versions.isEmpty()) {
            LOGGER.warn("emyty list of versions!");
            continue;
          }
          final long id = versions.get(0).getId();
          final Type type = versions.get(0).getType();

          if (type != Type.NODE)
            break;


          clearKeyValueCache();
          
          long minTimestamp = 0;
          Set<CellNode> cells = new HashSet<>();
          List<OSMNode> nodes = new ArrayList<>(versions.size());
          for (OSMPbfEntity entity : versions) {
            OSMPbfNode pbfNode = (OSMPbfNode) entity;
            nodes.add(getNode(pbfNode));

            minTimestamp = Math.min(minTimestamp, pbfNode.getTimestamp());
            if(pbfNode.getVisible())
              cells.add(getCell(pbfNode.getLongitude(), pbfNode.getLatitude()));
          }
          HOSMNode hnode = HOSMNode.build(nodes);

          for (CellNode cell : cells) {
            if (hnode.hasTags()) {
              cell.add(hnode,minTimestamp);
            }
          }

          Object[] ways = getRelation(getWaysForNode, hnode.getId());
          Object[] relation = getRelation(getRelationsForNode, hnode.getId());

          
          if (ways.length > 0)
            nodesForWays.add(new NodeRelation(((Long)ways[ways.length-1]).longValue(), hnode));
          if (relation.length > 0)
            nodesForRelations.add(new NodeRelation(((Long)relation[relation.length-1]).longValue(), hnode));
        } // while blockItr.hasNext();
        
        
        final SortedSet<CellNode> cellNodeOutput = new TreeSet<>();
        for(CellNode cellNode : mapCellnodes.values()){
          if(!cellNode.getNodes().isEmpty()){
            cellNodeOutput.add(cellNode);
          } 
        }

        return new Result(cellNodeOutput,nodesForWays,nodesForRelations);
        
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (

    SQLException e) {
      e.printStackTrace();
    }

    return EMPTY_RESULT;
  }

  

  private OSMNode getNode(OSMPbfNode entity) {
    return new OSMNode(entity.getId(), //
        entity.getVersion() * (entity.getVisible()?1:-1), //
        entity.getTimestamp(), //
        entity.getChangeset(), //
        entity.getUser().getId(), //
        getKeyValue(entity.getTags()), //
        entity.getLongitude(), entity.getLatitude());
  }

  private CellNode getCell(long longitude, long latitude) {
    
    long cellId = grid.getId(longitude*OSMNode.GEOM_PRECISION, latitude*OSMNode.GEOM_PRECISION);
    
    CellNode cellNode = mapCellnodes.get(cellId);
    if(cellNode == null){
      cellNode = new CellNode(cellId, grid.getLevel());
      mapCellnodes.put(cellId, cellNode );
    }
    return cellNode;
  }

}
