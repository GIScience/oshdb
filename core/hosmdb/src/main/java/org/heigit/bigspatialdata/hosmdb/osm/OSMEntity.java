package org.heigit.bigspatialdata.hosmdb.osm;

import java.util.Arrays;

public abstract class OSMEntity {
  protected final long id;

  protected final int version;
  protected final long timestamp;
  protected final long changeset;
  protected final int userId;
  protected final int[] tags;

  public OSMEntity(final long id, final int version, final long timestamp, final long changeset,
      final int userId, final int[] tags) {
    this.id = id;
    this.version = version;
    this.timestamp = timestamp;
    this.changeset = changeset;
    this.userId = userId;
    this.tags = tags;
  }

  public long getId() {
    return id;
  }


  public int getVersion() {
    return Math.abs(version);
  }


  public long getTimestamp() {
    return timestamp;
  }


  public long getChangeset() {
    return changeset;
  }


  public int getUserId() {
    return userId;
  }


  public boolean isVisible() {
    return (version >= 0);
  }


  public int[] getTags() {
    return tags;
  }
 
  public boolean equalsTo(OSMEntity o) {
    return id == o.id && version == o.version && timestamp == o.timestamp
        && changeset == o.changeset && userId == o.userId && Arrays.equals(tags, o.tags);
  }

  @Override
  public String toString() {
    return String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s USER:%d TAGS:%S", id, getVersion(),
        getTimestamp(), getChangeset(), isVisible(), getUserId(), Arrays.toString(getTags()));
  }

}
