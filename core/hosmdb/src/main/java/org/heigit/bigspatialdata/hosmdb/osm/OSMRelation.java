package org.heigit.bigspatialdata.hosmdb.osm;

import java.io.Serializable;


public class OSMRelation extends OSMEntity implements Comparable<OSMRelation>, Serializable {


  private static final long serialVersionUID = 1L;
  private final OSMMember[] members;

  public OSMRelation(final long id, final int version, final long timestamp, final long changeset,
      final int userId, final int[] tags, final OSMMember[] members) {
    super(id, version, timestamp, changeset, userId, tags);
    this.members = members;
  }


  public OSMMember[] getMembers() {
    return members;
  }

  @Override
  public int compareTo(OSMRelation o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(version, o.version);
      if (c == 0) {
        c = Long.compare(timestamp, o.timestamp);
      }
    }
    return c;
  }

}
