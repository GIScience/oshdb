package org.heigit.bigspatialdata.oshdb.osm2;

import org.heigit.bigspatialdata.oshdb.osh2.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public interface OSMMember {
  public long getId();

  public OSMType getType();

  public int getRoleId();
  
  public OSHEntity getEntity();
  
  public default String asString(){
    return String.format("%s:%d (%d)",getType(),getId(),getRoleId());
  }
  
}
