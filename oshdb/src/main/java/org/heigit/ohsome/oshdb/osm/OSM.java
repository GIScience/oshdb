package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTags;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntities;

/**
 * Factory utility class for single version osm-elements.
 */
@SuppressWarnings("squid:S107")
public class OSM {
  private OSM() {}

  /**
   * Creates a new {@code OSMNode} instance.
   */
  public static OSMNode node(final long id, final int version, final long timestamp,
      final long changeset, final int userId, final int[] tags, final int longitude,
      final int latitude) {
    return new Node(id, version, timestamp, changeset, userId, tags, longitude, latitude);
  }

  /**
   * Creates a new {@code OSMWay} instance.
   */
  public static OSMWay way(final long id, final int version, final long timestamp,
      final long changeset, final int userId, final int[] tags, final OSMMember[] refs) {
    return new Way(id, version, timestamp, changeset, userId, tags, refs);
  }

  /**
   * Creates a new {@code OSMRelation} instance.
   */
  public static OSMRelation relation(final long id, final int version, final long timestamp,
      final long changeset, final int userId, final int[] tags, final OSMMember[] members) {
    return new Relation(id, version, timestamp, changeset, userId, tags, members);
  }

  private abstract static class Entity implements OSMEntity, Comparable<OSMEntity> {

    protected final long id;

    protected final int version;
    protected final long timestamp;
    protected final long changesetId;
    protected final int userId;
    protected final OSHDBTags tags;

    /**
     * Constructor for a OSMEntity. Holds the basic information, every OSM-Object has.
     *
     * @param id ID
     * @param version Version. Versions &lt;=0 define visible Entities, &gt;0 deleted Entities.
     * @param timestamp Timestamp in seconds since 01.01.1970 00:00:00 UTC.
     * @param changesetId Changeset-ID
     * @param userId UserID
     * @param tags An array of OSHDB key-value ids. The format is [KID1,VID1,KID2,VID2...KIDn,VIDn].
     */
    protected Entity(final long id, final int version, final long timestamp, final long changesetId,
        final int userId, final int[] tags) {
      this.id = id;
      this.version = version;
      this.timestamp = timestamp;
      this.changesetId = changesetId;
      this.userId = userId;
      this.tags = OSHDBTags.of(tags);
    }

    public long getId() {
      return id;
    }

    public int getVersion() {
      return Math.abs(version);
    }

    @Override
    public long getEpochSecond() {
      return timestamp;
    }

    public long getChangesetId() {
      return changesetId;
    }

    public int getUserId() {
      return userId;
    }

    public boolean isVisible() {
      return version >= 0;
    }

    /**
     * Returns a "view" of the current osm tags.
     */
    public OSHDBTags getTags() {
      return tags;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getType(), id, version);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof OSMEntity)) {
        return false;
      }
      OSMEntity other = (OSMEntity) obj;
      return getType() == other.getType() && id == other.getId()
          && getVersion() == other.getVersion();
    }

    @Override
    public int compareTo(OSMEntity o) {
      int c = Long.compare(id, o.getId());
      if (c == 0) {
        c = Integer.compare(getVersion(), o.getVersion());
      }
      if (c == 0) {
        c = Long.compare(getEpochSecond(), o.getEpochSecond());
      }
      return c;
    }

    @Override
    public String toString() {
      return String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s UID:%d TAGS:%S", getId(), getVersion(),
          getEpochSecond(), getChangesetId(), isVisible(), getUserId(), getTags());
    }
  }

  private static class Node extends Entity implements OSMNode, Serializable {

    private static final long serialVersionUID = 1L;

    private final int longitude;
    private final int latitude;

    /**
     * Creates a new {@code OSMNode} instance.
     */
    private Node(final long id, final int version, final long timestamp, final long changeset,
        final int userId, final int[] tags, final int longitude, final int latitude) {
      super(id, version, timestamp, changeset, userId, tags);
      this.longitude = longitude;
      this.latitude = latitude;
    }

    public double getLongitude() {
      return OSMCoordinates.toWgs84(longitude);
    }

    public double getLatitude() {
      return OSMCoordinates.toWgs84(latitude);
    }

    public int getLon() {
      return longitude;
    }

    public int getLat() {
      return latitude;
    }

    @Override
    public String toString() {
      return String.format(Locale.ENGLISH, "NODE: %s %.7f:%.7f", super.toString(), getLongitude(),
          getLatitude());
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + Objects.hash(latitude, longitude);
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
      if (!(obj instanceof OSMNode)) {
        return false;
      }
      OSMNode other = (OSMNode) obj;
      return latitude == other.getLat() && longitude == other.getLon();
    }
  }

  private static class Way extends Entity implements OSMWay, Serializable {

    private static final long serialVersionUID = 1L;
    private final OSMMember[] members;

    public Way(final long id, final int version, final long timestamp, final long changeset,
        final int userId, final int[] tags, final OSMMember[] refs) {
      super(id, version, timestamp, changeset, userId, tags);
      this.members = refs;
    }

    /**
     * Returns the members for this current version.
     *
     * @return OSMMember for this version
     */
    public OSMMember[] getMembers() {
      return members;
    }

    /**
     * Returns a stream of all member entities (OSM) for the given timestamp.
     *
     * @param timestamp the timestamp for the osm member entity
     * @return stream of member entities (OSM)
     */
    public Stream<OSMNode> getMemberEntities(OSHDBTimestamp timestamp) {
      return Arrays.stream(this.getMembers()).map(OSMMember::getEntity).filter(Objects::nonNull)
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
      return Arrays.equals(members, other.getMembers());
    }
  }

  private static class Relation extends Entity implements OSMRelation, Serializable {

    private static final long serialVersionUID = 1L;
    private final OSMMember[] members;

    private Relation(final long id, final int version, final long timestamp, final long changeset,
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

    public Stream<OSMEntity> getMemberEntities(OSHDBTimestamp timestamp,
        Predicate<OSMMember> memberFilter) {
      return Arrays.stream(this.getMembers()).filter(memberFilter).map(OSMMember::getEntity)
          .filter(Objects::nonNull).map(entity -> OSHEntities.getByTimestamp(entity, timestamp));
    }

    /**
     * Returns a stream of all member entities (OSM) for the given timestamp.
     *
     * @param timestamp the timestamp for the osm member entity
     * @return stream of member entities (OSM)
     */
    public Stream<OSMEntity> getMemberEntities(OSHDBTimestamp timestamp) {
      return this.getMemberEntities(timestamp, osmMember -> true);
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
      if (!(obj instanceof OSMRelation)) {
        return false;
      }
      OSMRelation other = (OSMRelation) obj;
      return Arrays.equals(members, other.getMembers());
    }

    @Override
    public String toString() {
      return String.format("Relation-> %s Mem:%s", super.toString(), Arrays.toString(getMembers()));
    }
  }
}
