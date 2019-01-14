package org.heigit.bigspatialdata.oshdb.util.changesetenricher;

import java.util.Objects;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSMChangesetComment {
  private final long comment_changeset_id;
  private final Long comment_user_id;
  private final String comment_user_name;
  private final OSHDBTimestamp comment_date;

  private final String comment_text;

  public OSMChangesetComment(long comment_changeset_id, long comment_user_id, String comment_user_name, OSHDBTimestamp comment_date, String comment_text) {
    this.comment_changeset_id = comment_changeset_id;
    this.comment_user_id = comment_user_id;
    this.comment_user_name = comment_user_name;
    this.comment_date = comment_date;
    this.comment_text = comment_text;
  }

  /**
   * @return the comment_changeset_id
   */
  public long getComment_changeset_id() {
    return comment_changeset_id;
  }

  /**
   * @return the comment_user_id
   */
  public long getComment_user_id() {
    return comment_user_id;
  }

  /**
   * @return the comment_user_name
   */
  public String getComment_user_name() {
    return comment_user_name;
  }

  /**
   * @return the comment_date
   */
  public OSHDBTimestamp getComment_date() {
    return comment_date;
  }

  /**
   * @return the comment_text
   */
  public String getComment_text() {
    return comment_text;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 19 * hash + (int) (this.comment_changeset_id ^ (this.comment_changeset_id >>> 32));
    hash = 19 * hash + Objects.hashCode(this.comment_user_id);
    hash = 19 * hash + Objects.hashCode(this.comment_user_name);
    hash = 19 * hash + Objects.hashCode(this.comment_date);
    hash = 19 * hash + Objects.hashCode(this.comment_text);
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
    final OSMChangesetComment other = (OSMChangesetComment) obj;
    if (this.comment_changeset_id != other.comment_changeset_id) {
      return false;
    }
    if (!Objects.equals(this.comment_user_name, other.comment_user_name)) {
      return false;
    }
    if (!Objects.equals(this.comment_text, other.comment_text)) {
      return false;
    }
    if (!Objects.equals(this.comment_user_id, other.comment_user_id)) {
      return false;
    }
    if (!Objects.equals(this.comment_date, other.comment_date)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "OSMChangesetComment{" + "comment_changeset_id=" + comment_changeset_id + ", comment_user_id=" + comment_user_id + ", comment_user_name=" + comment_user_name + ", comment_date=" + comment_date + ", comment_text=" + comment_text + '}';
  }

}
