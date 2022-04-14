package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import java.util.Objects;
import org.heigit.ohsome.oshdb.OSHDBTags;
import org.heigit.ohsome.oshdb.OSHDBTemporal;

public abstract class OSMEntity implements OSHDBTemporal, Serializable {

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
  protected OSMEntity(final long id, final int version, final long timestamp,
      final long changesetId, final int userId, final int[] tags) {
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

  public abstract OSMType getType();

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
    return getType() == other.getType() && id == other.id && version == other.version;
  }

  @Override
  public String toString() {
    return String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s UID:%d TAGS:%S", getId(), getVersion(),
        getEpochSecond(), getChangesetId(), isVisible(), getUserId(), getTags());
  }
}
