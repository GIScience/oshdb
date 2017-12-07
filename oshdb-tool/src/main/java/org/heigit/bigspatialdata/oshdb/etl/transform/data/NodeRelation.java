package org.heigit.bigspatialdata.oshdb.etl.transform.data;

import java.io.Serializable;

import org.heigit.bigspatialdata.oshdb.osh.OSHNode;

public class NodeRelation implements Serializable, Comparable<NodeRelation> {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final long maxRelationId;
  
  private final OSHNode hnode;
  
  public NodeRelation(final long maxRelationId,final OSHNode hnode){
    this.maxRelationId = maxRelationId;
    this.hnode = hnode;
  }
  
  public OSHNode node(){
    return hnode;
  }
  
  public long getMaxRelationId(){
    return maxRelationId;
  }

  @Override
  public int compareTo(NodeRelation o) {
    int c = Long.compare(maxRelationId, o.maxRelationId);
    if(c == 0)
      c = Long.compare(hnode.getId(), o.hnode.getId());
    return c;
  }

 
  
}
