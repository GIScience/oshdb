package org.heigit.bigspatialdata.oshdb.osm;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSMWay extends OSMEntity implements Comparable<OSMWay>, Serializable {

  private static final long serialVersionUID = 1L;
  private final OSMMember[] refs;

  public OSMWay(final long id, final int version, final OSHDBTimestamp timestamp, final long changeset,
      final int userId, final int[] tags, final OSMMember[] refs) {
    super(id, version, timestamp, changeset, userId, tags);
    this.refs = refs;
  }

  @Override
  public OSMType getType() {
    return OSMType.WAY;
  }

  public OSMMember[] getRefs() {
    return refs;
  }

  public Stream<OSMNode> getRefEntities(OSHDBTimestamp timestamp) {
    return Arrays.stream(this.getRefs()).map(OSMMember::getEntity).filter(Objects::nonNull)
        .map(entity -> ((OSHNode) entity).getByTimestamp(timestamp));
  }

  @Override
  public String toString() {
    return String.format("WAY-> %s Refs:%s", super.toString(), Arrays.toString(getRefs()));
  }

  @Override
  public int compareTo(OSMWay o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
    }
    if (c == 0) {
      c = Long.compare(timestamp.getRawUnixTimestamp(), o.timestamp.getRawUnixTimestamp());
    }
    return c;
  }

}
