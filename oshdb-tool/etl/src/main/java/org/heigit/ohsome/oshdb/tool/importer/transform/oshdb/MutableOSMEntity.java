package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;

public class MutableOSMEntity implements OSMEntity {

  private long id;

  private int version;
  private boolean visible;
  private long timestamp;
  private long changeset;
  private int userId;
  private int[] tags;

  public void setEntity(long id, int version, boolean visible, long timestamp, long changeset,
      int userId, int[] tags) {
    this.id = id;
    this.version = version;
    this.visible = visible;
    this.timestamp = timestamp;
    this.changeset = changeset;
    this.userId = userId;
    this.tags = tags;
  }


  @Override
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public boolean isVisible() {
    return visible;
  }

  public void isVisible(boolean visible) {
    this.visible = visible;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public long getEpochSecond() {
    return timestamp;
  }

  public void setTimestamp(OSHDBTimestamp timestamp) {
    this.timestamp = timestamp.getEpochSecond();
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public long getChangeset() {
    return changeset;
  }

  public void setChangeset(long changeset) {
    this.changeset = changeset;
  }

  @Override
  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  @Override
  public int[] getTags() {
    return tags;
  }

  public void setTags(int[] tags) {
    this.tags = tags;
  }

  @Override
  public String toString() {
    return asString();
  }

}
