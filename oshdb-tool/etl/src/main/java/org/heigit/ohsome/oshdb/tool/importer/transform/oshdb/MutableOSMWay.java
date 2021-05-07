package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

public class MutableOSMWay extends MutableOSMEntity implements OSMWay {

  private OSMMember[] members;

  @Override
  public OSMMember[] getMembers() {
    return members;
  }

  public void setExtension(OSMMember[] members) {
    this.members = members;
  }
}
