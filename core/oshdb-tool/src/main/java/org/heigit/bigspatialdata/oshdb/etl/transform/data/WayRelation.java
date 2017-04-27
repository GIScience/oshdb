package org.heigit.bigspatialdata.hosmdb.etl.transform.data;

import java.io.Serializable;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMWay;

public class WayRelation implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final long maxRelationId;
  private final HOSMWay hosmWay;
  
  public WayRelation(final long maxRelationId, final HOSMWay hosmWay){
    this.maxRelationId = maxRelationId;
    this.hosmWay = hosmWay;
  }
  
  public HOSMWay way(){
    return hosmWay;
  }
  
  public long getMaxRelationId(){
    return maxRelationId;
  }
}
