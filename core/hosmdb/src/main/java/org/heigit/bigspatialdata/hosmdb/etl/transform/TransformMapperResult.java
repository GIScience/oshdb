package org.heigit.bigspatialdata.hosmdb.etl.transform;

import java.util.List;
import java.util.SortedSet;

import org.heigit.bigspatialdata.hosmdb.etl.transform.data.CellNode;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.NodeRelation;

public class TransformMapperResult {
  
  private final SortedSet<CellNode>  nodeCells;
  private final List<NodeRelation> nodesForWays;
  private final List<NodeRelation> nodesForRelations;
  
  
  
  public TransformMapperResult(final SortedSet<CellNode> nodeCells,final List<NodeRelation> nodesForWays, final List<NodeRelation> nodesForRelations ) {
    this.nodeCells = nodeCells;
    this.nodesForWays = nodesForWays;
    this.nodesForRelations = nodesForRelations;
  }
  

}
