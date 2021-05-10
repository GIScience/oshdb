package org.heigit.ohsome.oshdb.util.xmlreader;

import org.heigit.ohsome.oshdb.osm.OSMMember;

/**
 * A mutable OSM relation, specifically for use in {@link OSMXmlReader}.
 */
public class MutableOSMRelation extends MutableOSMEntity  {
  private OSMMember[] members;

  public OSMMember[] getMembers() {
    return members;
  }

  public void setExtension(OSMMember[] members) {
    this.members = members;
  }
}
