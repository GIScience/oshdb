package org.heigit.bigspatialdata.hosmdb.etl.transform.data;

import java.io.Serializable;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;

public class NodeRelation implements Serializable, Comparable<NodeRelation> {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final long maxRelationId;
  
  private final HOSMNode hnode;
  
  public NodeRelation(final long maxRelationId,final HOSMNode hnode){
    this.maxRelationId = maxRelationId;
    this.hnode = hnode;
  }
  
  public HOSMNode node(){
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
