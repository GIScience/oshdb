package org.heigit.ohsome.oshdb.util.xmlreader;

import org.heigit.ohsome.oshdb.osm.OSMMember;

/**
 * A mutable OSM way, specifically for use in {@link OSMXmlReader}.
 */
public class MutableOSMWay extends MutableOSMEntity {
  private OSMMember[] members;

  public OSMMember[] getMembers() {
    return members;
  }

  public void setExtension(OSMMember[] members) {
    this.members = members;
  }
}
