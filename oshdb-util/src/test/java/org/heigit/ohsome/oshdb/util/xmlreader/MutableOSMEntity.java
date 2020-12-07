package org.heigit.ohsome.oshdb.util.xmlreader;

import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;

public class MutableOSMEntity {

  private long id;

  private int version;
  private boolean visible;
  private OSHDBTimestamp timestamp = new OSHDBTimestamp(0L);
  private long changeset;
  private int userId;
  private int[] tags;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public int getVersion() {
    return version;
  }
  
  public boolean isVisible() {
    return visible;
  }
  
  public void isVisible(boolean visible) {
    this.visible = visible;
  }
  

  public void setVersion(int version) {
    this.version = version;
  }

  public OSHDBTimestamp getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(OSHDBTimestamp timestamp) {
    this.timestamp = timestamp;
  }
  
  public void setTimestamp(long timestamp) {
    this.timestamp.setTimestamp(timestamp);
  }

  public long getChangeset() {
    return changeset;
  }

  public void setChangeset(long changeset) {
    this.changeset = changeset;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  public int[] getTags() {
    return tags;
  }

  public void setTags(int[] tags) {
    this.tags = tags;
  }

}
