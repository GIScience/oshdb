package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntities;

public class OSMWay extends OSMEntity implements Comparable<OSMWay>, Serializable {

  private static final long serialVersionUID = 1L;
  private final OSMMember[] members;

  public OSMWay(final long id, final int version, final long timestamp, final long changeset,
      final int userId, final int[] tags, final OSMMember[] refs) {
    super(id, version, timestamp, changeset, userId, tags);
    this.members = refs;
  }

  @Override
  public OSMType getType() {
    return OSMType.WAY;
  }

  /**
   * Returns the members for this current version.
   *
   * @return OSMMember for this version
   */
  public OSMMember[] getMembers() {
    return members;
  }

  public Stream<OSMNode> getRefEntities(OSHDBTimestamp timestamp) {
    return Arrays.stream(this.getMembers())
        .map(OSMMember::getEntity)
        .filter(Objects::nonNull)
        .map(entity -> (OSMNode) OSHEntities.getByTimestamp(entity, timestamp));
  }

  @Override
  public String toString() {
    return String.format("WAY-> %s Refs:%s", super.toString(), Arrays.toString(getMembers()));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Arrays.hashCode(members);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof OSMWay)) {
      return false;
    }
    OSMWay other = (OSMWay) obj;
    return Arrays.equals(members, other.members);
  }

  @Override
  public int compareTo(OSMWay o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
    }
    if (c == 0) {
      c = Long.compare(timestamp, o.timestamp);
    }
    return c;
  }

}
