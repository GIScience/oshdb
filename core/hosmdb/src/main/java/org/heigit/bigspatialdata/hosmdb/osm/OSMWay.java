package org.heigit.bigspatialdata.hosmdb.osm;

import java.io.Serializable;
import java.util.Arrays;


public class OSMWay extends OSMEntity implements Comparable<OSMWay>, Serializable {

  private static final long serialVersionUID = 1L;
  private final OSMMember[] refs;

  public OSMWay(final long id, final int version, final long timestamp, final long changeset,
      final int userId, final int[] tags, final OSMMember[] refs) {
    super(id, version, timestamp, changeset, userId, tags);
    this.refs = refs;
  }


  public OSMMember[] getRefs() {
    return refs;
  }
  
  @Override
  public String toString() {
    return String.format("WAY-> %s Refs:%s", super.toString(), Arrays.toString(getRefs()));
  }


  @Override
  public int compareTo(OSMWay o) {
    int c = Long.compare(id, o.id);
    if (c == 0)
      c = Integer.compare(version, o.version);
    if (c == 0)
      c = Long.compare(timestamp, o.timestamp);
    return c;
  }

}
