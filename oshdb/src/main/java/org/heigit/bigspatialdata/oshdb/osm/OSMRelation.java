package org.heigit.bigspatialdata.oshdb.osm;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSMRelation extends OSMEntity implements Comparable<OSMRelation>, Serializable {

  private static final long serialVersionUID = 1L;
  private final OSMMember[] members;

  public OSMRelation(final long id, final int version, final OSHDBTimestamp timestamp, final long changeset,
      final int userId, final int[] tags, final OSMMember[] members) {
    super(id, version, timestamp, changeset, userId, tags);
    this.members = members;
  }

  @Override
  public OSMType getType() {
    return OSMType.RELATION;
  }

  public OSMMember[] getMembers() {
    return members;
  }

  public Stream<OSMEntity> getMemberEntities(OSHDBTimestamp timestamp, Predicate<OSMMember> memberFilter) {
    return Arrays.stream(this.getMembers()).filter(memberFilter).map(OSMMember::getEntity)
        .filter(Objects::nonNull).map(entity -> entity.getByTimestamp(timestamp));
  }

  public Stream<OSMEntity> getMemberEntities(OSHDBTimestamp timestamp) {
    return this.getMemberEntities(timestamp, osmMember -> true);
  }

  @Override
  public int compareTo(OSMRelation o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
      if (c == 0) {
        c = Long.compare(timestamp.getRawUnixTimestamp(), o.timestamp.getRawUnixTimestamp());
      }
    }
    return c;
  }

  @Override
  public String toString() {
    return String.format("Relation-> %s Mem:%s", super.toString(), Arrays.toString(getMembers()));
  }

}
