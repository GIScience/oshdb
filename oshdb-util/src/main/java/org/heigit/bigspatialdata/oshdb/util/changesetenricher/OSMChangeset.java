package org.heigit.bigspatialdata.oshdb.util.changesetenricher;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSMChangeset {
  private final long id;
  private final Long user_id;
  private final OSHDBTimestamp created_at;
  private final OSHDBBoundingBox bbx;
  private final OSHDBTimestamp closed_at;
  private final Boolean open;
  private final Integer num_changes;
  private final String user_name;
  private final Map<String, String> tags;
  private final List<OSMChangesetComment> comments;

  public OSMChangeset(long id, long user_id, OSHDBTimestamp chreated_at, OSHDBBoundingBox bbx, OSHDBTimestamp closed_at, boolean open, int num_changes, String user_name, Map<String, String> tags, List<OSMChangesetComment> comments) {
    this.id = id;
    this.user_id = user_id;
    this.created_at = chreated_at;
    this.bbx = bbx;
    this.closed_at = closed_at;
    this.open = open;
    this.num_changes = num_changes;
    this.user_name = user_name;
    this.tags = tags;
    this.comments = comments;
  }

  /**
   * @return the id
   */
  public long getId() {
    return id;
  }

  /**
   * @return the user_id
   */
  public Long getUser_id() {
    return user_id;
  }

  /**
   * @return the created_at
   */
  public OSHDBTimestamp getCreated_at() {
    return created_at;
  }

  /**
   * @return the bbx
   */
  public OSHDBBoundingBox getBbx() {
    return bbx;
  }

  /**
   * @return the closed_at
   */
  public OSHDBTimestamp getClosed_at() {
    return closed_at;
  }

  /**
   * @return the open
   */
  public Boolean isOpen() {
    return open;
  }

  /**
   * @return the num_changes
   */
  public Integer getNum_changes() {
    return num_changes;
  }

  /**
   * @return the user_name
   */
  public String getUser_name() {
    return user_name;
  }

  /**
   * @return the tags
   */
  public Map<String, String> getTags() {
    return tags;
  }

  /**
   * @return the comments
   */
  public List<OSMChangesetComment> getComments() {
    return comments;
  }

  @Override
  public String toString() {
    return "OSMChangeset{" + "id=" + id + ", user_id=" + user_id + ", created_at=" + created_at + ", bbx=" + bbx + ", closed_at=" + closed_at + ", open=" + open + ", num_changes=" + num_changes + ", user_name=" + user_name + ", tags=" + tags + ", comments=" + comments + '}';
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + (int) (this.id ^ (this.id >>> 32));
    hash = 29 * hash + Objects.hashCode(this.user_id);
    hash = 29 * hash + Objects.hashCode(this.created_at);
    hash = 29 * hash + Objects.hashCode(this.bbx);
    hash = 29 * hash + Objects.hashCode(this.closed_at);
    hash = 29 * hash + Objects.hashCode(this.open);
    hash = 29 * hash + Objects.hashCode(this.num_changes);
    hash = 29 * hash + Objects.hashCode(this.user_name);
    hash = 29 * hash + Objects.hashCode(this.tags);
    hash = 29 * hash + Objects.hashCode(this.comments);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OSMChangeset other = (OSMChangeset) obj;
    if (this.id != other.id) {
      return false;
    }
    if (!Objects.equals(this.user_name, other.user_name)) {
      return false;
    }
    if (!Objects.equals(this.user_id, other.user_id)) {
      return false;
    }
    if (!Objects.equals(this.created_at, other.created_at)) {
      return false;
    }
    if (!Objects.equals(this.bbx, other.bbx)) {
      return false;
    }
    if (!Objects.equals(this.closed_at, other.closed_at)) {
      return false;
    }
    if (!Objects.equals(this.open, other.open)) {
      return false;
    }
    if (!Objects.equals(this.num_changes, other.num_changes)) {
      return false;
    }
    if (!Objects.equals(this.tags, other.tags)) {
      return false;
    }
    if (!Objects.equals(this.comments, other.comments)) {
      return false;
    }
    return true;
  }

}
