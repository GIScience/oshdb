package org.heigit.bigspatialdata.hosmdb.etl.transform.data;

import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMRelation;


public class CellRelation implements Comparable<CellRelation> {

  private final CellInfo info;
private final List<HOSMRelation> relations = new ArrayList<>();
  
  private long minTimestamp;
  private long minId;
  
  public CellRelation(final long cellId, final int zoomLevel){
    this.info = new CellInfo(cellId,zoomLevel);
  }
  
  
  public CellInfo info(){
    return info;
  }
 
  public void add(HOSMRelation r, long minTimestamp){
    this.relations.add(r);
    minId = Math.min(minId, r.getId());
    minTimestamp = Math.min(this.minTimestamp,minTimestamp);
  }
  
  public List<HOSMRelation> getRelations(){
    return relations;
  }
  
  
  public long minId(){
    return minId;
  }
  
  public long minTimestamp(){
    return minTimestamp;
  }

  @Override
  public int compareTo(CellRelation o) {
   return info.compareTo(o.info);
  }
  
  @Override
  public int hashCode() {
    return info.hashCode();
  }
}
