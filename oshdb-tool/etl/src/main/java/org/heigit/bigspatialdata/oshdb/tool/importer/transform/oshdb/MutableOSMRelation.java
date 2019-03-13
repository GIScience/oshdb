package org.heigit.bigspatialdata.oshdb.tool.importer.transform.oshdb;

public class MutableOSMRelation extends MutableOSMEntity implements OSMRelation {

  private OSMMember[] members;
  
  @Override
  public OSMMember[] getMembers() {
    return members;
  }

  public void setExtension(OSMMember[] members) {
    this.members = members; 
  }

}
