package org.heigit.bigspatialdata.oshdb.util.changesetenricher;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSMChangeset {
  private final long id;
  private final Long userId;
  private final OSHDBTimestamp createdAt;
  private final OSHDBBoundingBox bbx;
  private final OSHDBTimestamp closedAt;
  private final Boolean open;
  private final Integer numChanges;
  private final String userName;
  private final Map<String, String> tags;
  private final List<OSMChangesetComment> comments;

  /**
   * An OSM Changeset containing all metadata.
   *
   * @param id The changeset-id
   * @param userId The user-id of the user that created the changeset
   * @param createdAt The timestamp the changeset was created at
   * @param bbx The boundingbox of all edits contained in the changeset
   * @param closedAt The timestamp the changeset was closed (if any)
   * @param open The status of the changeset
   * @param numChanges The number of changes made
   * @param userName The string-representation of the user-id
   * @param tags Additional tags the changeset has
   * @param comments Comments made in the changeset discussion
   */
  public OSMChangeset(long id, long userId, OSHDBTimestamp createdAt, OSHDBBoundingBox bbx,
      OSHDBTimestamp closedAt, boolean open, int numChanges, String userName,
      Map<String, String> tags, List<OSMChangesetComment> comments) {
    this.id = id;
    this.userId = userId;
    this.createdAt = createdAt;
    this.bbx = bbx;
    this.closedAt = closedAt;
    this.open = open;
    this.numChanges = numChanges;
    this.userName = userName;
    this.tags = tags;
    this.comments = comments;
  }

  /**
   * Get the changeset id.
   *
   * @return the id
   */
  public long getId() {
    return id;
  }

  /**
   * Get the user-id of the user that created the changeset.
   *
   * @return the userId
   */
  public Long getUserId() {
    return userId;
  }

  /**
   * Get the timestamp the changeset was created at.
   * 
   * @return the createdAt
   */
  public OSHDBTimestamp getCreatedAts() {
    return createdAt;
  }

  /**
   * Get the boundingbox of the changeset.
   * 
   * @return the bbx
   */
  public OSHDBBoundingBox getBbx() {
    return bbx;
  }

  /**
   * Get the timestamp the changeset was closed at (if any).
   * 
   * @return the closedAt
   */
  public OSHDBTimestamp getClosedAt() {
    return closedAt;
  }

  /**
   * Return the status of the changeset.
   * 
   * @return the open
   */
  public Boolean isOpen() {
    return open;
  }

  /**
   * Get the number of changes made.
   * 
   * @return the numChanges
   */
  public Integer getNumChanges() {
    return numChanges;
  }

  /**
   * Get the string user name.
   * 
   * @return the userName
   */
  public String getUserName() {
    return userName;
  }

  /**
   * Get tags of changeset.
   * 
   * @return the tags
   */
  public Map<String, String> getTags() {
    return tags;
  }

  /**
   * Get comments of changeset-discussion (if any).
   * 
   * @return the comments
   */
  public List<OSMChangesetComment> getComments() {
    return comments;
  }

  @Override
  public String toString() {
    return "OSMChangeset{" + "id=" + id + ", user_id=" + userId + ", created_at=" + createdAt + ", bbx=" + bbx + ", closed_at=" + closedAt + ", open=" + open + ", num_changes=" + numChanges + ", user_name=" + userName + ", tags=" + tags + ", comments=" + comments + '}';
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + (int) (this.id ^ (this.id >>> 32));
    hash = 29 * hash + Objects.hashCode(this.userId);
    hash = 29 * hash + Objects.hashCode(this.createdAt);
    hash = 29 * hash + Objects.hashCode(this.bbx);
    hash = 29 * hash + Objects.hashCode(this.closedAt);
    hash = 29 * hash + Objects.hashCode(this.open);
    hash = 29 * hash + Objects.hashCode(this.numChanges);
    hash = 29 * hash + Objects.hashCode(this.userName);
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
    if (!Objects.equals(this.userName, other.userName)) {
      return false;
    }
    if (!Objects.equals(this.userId, other.userId)) {
      return false;
    }
    if (!Objects.equals(this.createdAt, other.createdAt)) {
      return false;
    }
    if (!Objects.equals(this.bbx, other.bbx)) {
      return false;
    }
    if (!Objects.equals(this.closedAt, other.closedAt)) {
      return false;
    }
    if (!Objects.equals(this.open, other.open)) {
      return false;
    }
    if (!Objects.equals(this.numChanges, other.numChanges)) {
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
