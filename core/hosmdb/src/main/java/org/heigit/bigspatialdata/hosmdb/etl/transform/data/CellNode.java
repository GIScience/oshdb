package org.heigit.bigspatialdata.hosmdb.etl.transform.data;

import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;

public class CellNode implements Comparable<CellNode>{
  
  private final CellInfo info;
  private final List<HOSMNode> nodes = new ArrayList<>();
  
  private long minTimestamp;
  private long minId;
  
  public CellNode(final long cellId, final int zoomLevel){
    this.info = new CellInfo(cellId,zoomLevel);

  }
  
  public CellInfo info(){
    return info;
  }
 
  public void add(HOSMNode node, long minTimestamp){
    this.nodes.add(node);
    minId = Math.min(minId, node.getId());
    minTimestamp = Math.min(this.minTimestamp,minTimestamp);
  }
  
  public List<HOSMNode> getNodes(){
    return nodes;
  }
  
  public long minId(){
    return minId;
  }
  
  public long minTimestamp(){
    return minTimestamp;
  }

  @Override
  public int compareTo(CellNode o) {
   return info.compareTo(o.info);
  }
  
  @Override
  public int hashCode() {
    return info.hashCode();
  }
}
