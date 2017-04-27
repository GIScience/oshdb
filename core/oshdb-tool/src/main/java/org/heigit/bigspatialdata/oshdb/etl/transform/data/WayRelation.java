package org.heigit.bigspatialdata.oshdb.etl.transform.data;

import java.io.Serializable;

import org.heigit.bigspatialdata.oshdb.osh.OSHWay;

public class WayRelation implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final long maxRelationId;
  private final OSHWay oshWay;
  
  public WayRelation(final long maxRelationId, final OSHWay oshWay){
    this.maxRelationId = maxRelationId;
    this.oshWay = oshWay;
  }
  
  public OSHWay way(){
    return oshWay;
  }
  
  public long getMaxRelationId(){
    return maxRelationId;
  }
}
