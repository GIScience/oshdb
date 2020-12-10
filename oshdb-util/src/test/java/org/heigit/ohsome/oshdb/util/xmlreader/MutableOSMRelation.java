package org.heigit.ohsome.oshdb.util.xmlreader;

import org.heigit.ohsome.oshdb.osm.OSMMember;

public class MutableOSMRelation extends MutableOSMEntity  {

  private OSMMember[] members;
  
  public OSMMember[] getMembers() {
    return members;
  }

  public void setExtension(OSMMember[] members) {
    this.members = members; 
  }

}
