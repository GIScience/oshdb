package org.heigit.bigspatialdata.oshdb.osm2.impl;

import org.heigit.bigspatialdata.oshdb.osm2.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm2.OSMWay;

public class MutableOSMWay extends MutableOSMEntity implements OSMWay{

  private OSMMember[] members;
  
  @Override
  public OSMMember[] getMembers() {
    return members;
  }

  public void setExtension(OSMMember[] members) {
    this.members = members;
    
  }
}
