package org.heigit.bigspatialdata.oshdb.util.changesetenricher;

import java.util.Objects;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSMChangesetComment {
  private final long commentChangesetId;
  private final Long commentUserId;
  private final String commentUserName;
  private final OSHDBTimestamp commentDate;

  private final String commentText;

  /**
   * A changeset comment from a changeset discussion.
   *
   * @param commentChangesetId The id of the changeset the comment belongs to
   * @param commentUserId The id of the user that made the comment
   * @param commentUserName The name of the user that made the comment
   * @param commentDate The timestamp the comment was made
   * @param commentText The content of the comment
   */
  public OSMChangesetComment(
      long commentChangesetId,
      long commentUserId,
      String commentUserName,
      OSHDBTimestamp commentDate,
      String commentText) {
    this.commentChangesetId = commentChangesetId;
    this.commentUserId = commentUserId;
    this.commentUserName = commentUserName;
    this.commentDate = commentDate;
    this.commentText = commentText;
  }

  /**
   * Get the id of the changeset, this comment belongs to.
   * 
   * @return the comment changeset id
   */
  public long getCommentChangesetId() {
    return commentChangesetId;
  }

  /**
   * Get the user id of the user that made the comment.
   * 
   * @return the comment user id
   */
  public long getCommentUserId() {
    return commentUserId;
  }

  /**
   * Get the user name of the user that made the comment.
   * 
   * @return the comment user name
   */
  public String getCommentUserName() {
    return commentUserName;
  }

  /**
   * Get the Timestamp the comment was made at.
   * 
   * @return the comment date
   */
  public OSHDBTimestamp getCommentDate() {
    return commentDate;
  }

  /**
   * Get the actual comment (content).
   * 
   * @return the comment text
   */
  public String getCommentText() {
    return commentText;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 19 * hash + (int) (this.commentChangesetId ^ (this.commentChangesetId >>> 32));
    hash = 19 * hash + Objects.hashCode(this.commentUserId);
    hash = 19 * hash + Objects.hashCode(this.commentUserName);
    hash = 19 * hash + Objects.hashCode(this.commentDate);
    hash = 19 * hash + Objects.hashCode(this.commentText);
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
    if (this.commentChangesetId != other.commentChangesetId) {
      return false;
    }
    if (!Objects.equals(this.commentUserName, other.commentUserName)) {
      return false;
    }
    if (!Objects.equals(this.commentText, other.commentText)) {
      return false;
    }
    if (!Objects.equals(this.commentUserId, other.commentUserId)) {
      return false;
    }
    if (!Objects.equals(this.commentDate, other.commentDate)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "OSMChangesetComment{"
        + "comment_changeset_id=" + commentChangesetId
        + ", comment_user_id=" + commentUserId
        + ", comment_user_name=" + commentUserName
        + ", comment_date=" + commentDate
        + ", comment_text=" + commentText
        + '}';
  }

}
