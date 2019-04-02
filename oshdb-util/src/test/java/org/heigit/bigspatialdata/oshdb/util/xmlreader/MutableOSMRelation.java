package org.heigit.bigspatialdata.oshdb.util.xmlreader;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;

public class MutableOSMRelation extends MutableOSMEntity  {

  private OSMMember[] members;
  
  public OSMMember[] getMembers() {
    return members;
  }

  public void setExtension(OSMMember[] members) {
    this.members = members; 
  }

}
