package org.heigit.bigspatialdata.oshdb.util.changesetenricher;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.slf4j.LoggerFactory;

public class ChangesetEnricher {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ChangesetEnricher.class);

  private final Connection conn;
  private final Map<Long, OSMChangeset> changests;

  /**
   * This class works as a cache (like
   * {@link org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator}). Attention: The
   * ChangesetDatabase is minutely updated but the cache isn't. So subsequent calls to this Class
   * for the same ID will always return the same result. If you want to update the cache you will
   * have to create a new instance.
   *
   * @param conn The connection to the ohsome-changeset postgres-database.
   */
  public ChangesetEnricher(Connection conn) {
    this.changests = new ConcurrentHashMap<>(0);
    this.conn = conn;
  }

  /**
   * Get latest update Timestamp of Changeset-Database (may be null if unknown).
   *
   * @return The timestamp of the last update performed on the queried db
   * @throws SQLException If the db could not be queried
   */
  public OSHDBTimestamp getValidUntil() throws SQLException {

    Timestamp timestamp;
    try (PreparedStatement prepareStatement
        = this.conn.prepareStatement("SELECT last_timestamp FROM osm_changeset_state")) {
      ResultSet executeQuery = prepareStatement.executeQuery();
      if (executeQuery.next()) {
        timestamp = executeQuery.getTimestamp(1);
      } else {
        timestamp = null;
      }
    }
    if (timestamp == null) {
      return null;
    }
    return new OSHDBTimestamp(timestamp);
  }

  /**
   * Get a Changeset with comments. May be null if changesetId not in Database. Any field may also
   * be null.
   *
   * @param changesetId The id of the changeset to be enriched
   * @return The enriched changeset corresponding to the given id
   */
  public OSMChangeset getChangeset(long changesetId) throws SQLException {
    if (this.changests.containsKey(changesetId)) {
      return this.changests.get(changesetId);
    }
    OSMChangeset changeset;
    try (PreparedStatement prepareStatement = this.conn.prepareStatement(
        "SELECT "
          + "user_id,"
          + "created_at,"
          + "min_lat,"
          + "max_lat,"
          + "min_lon,"
          + "max_lon,"
          + "closed_at,"
          + "open,"
          + "num_changes,"
          + "user_name,"
          + "tags "
        + "FROM osm_changeset "
        + "WHERE id = ? ;")) {
      prepareStatement.setLong(1, changesetId);
      ResultSet resultSet = prepareStatement.executeQuery();

      if (resultSet.next()) {
        final Long userId = resultSet.getLong("user_id");

        OSHDBTimestamp createdAt;
        try {
          createdAt = new OSHDBTimestamp(resultSet.getTimestamp("created_at"));
        } catch (NullPointerException ex) {
          createdAt = null;
        }

        OSHDBBoundingBox bbx;
        try {
          bbx = new OSHDBBoundingBox(
              resultSet.getDouble("min_lon"),
              resultSet.getDouble("min_lat"),
              resultSet.getDouble("max_lon"),
              resultSet.getDouble("max_lat"));
        } catch (NullPointerException ex) {
          bbx = null;
        }

        OSHDBTimestamp closedAt;
        try {
          closedAt = new OSHDBTimestamp(resultSet.getTimestamp("closed_at"));
        } catch (NullPointerException ex) {
          closedAt = null;
        }

        Boolean open = resultSet.getBoolean("open");

        Integer numChanges = resultSet.getInt("num_changes");

        String userName = resultSet.getString("user_name");

        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) resultSet.getObject("tags");

        List<OSMChangesetComment> commentList = this.getChangesetComments(changesetId);

        changeset = new OSMChangeset(
            changesetId,
            userId,
            createdAt,
            bbx,
            closedAt,
            open,
            numChanges,
            userName,
            tags,
            commentList);

      } else {
        LOG.info("Unable to find changeset-id {} in database.", changesetId);
        return null;
      }

    }
    this.changests.put(changesetId, changeset);
    return changeset;

  }

  /**
   * Get comments of a specified changeset. May be null if no comments exist. Any field may also be
   * null.
   *
   * @param changesetId The id of the changeset the comments are requested for
   * @return A list of all changeset comments
   * @throws SQLException If the db could not be queried
   */
  public List<OSMChangesetComment> getChangesetComments(long changesetId) throws SQLException {
    List<OSMChangesetComment> commentList = new ArrayList<>(1);
    try (PreparedStatement prepareStatement = this.conn.prepareStatement(
        "SELECT comment_user_id,comment_user_name,comment_date,comment_text "
        + "FROM osm_changeset_comment "
        + "WHERE comment_changeset_id = ? ;")) {
      prepareStatement.setLong(1, changesetId);
      ResultSet resultSet = prepareStatement.executeQuery();

      if (!resultSet.isBeforeFirst()) {
        return null;
      } else {
        while (resultSet.next()) {
          Long commentUserId = resultSet.getLong("comment_user_id");

          String commentUserName = resultSet.getString("comment_user_name");

          OSHDBTimestamp commentDate;
          try {
            commentDate = new OSHDBTimestamp(resultSet.getTimestamp("comment_date"));
          } catch (NullPointerException ex) {
            commentDate = null;
          }

          String commentText = resultSet.getString("comment_text");

          OSMChangesetComment comment = new OSMChangesetComment(
              changesetId,
              commentUserId,
              commentUserName,
              commentDate,
              commentText);
          commentList.add(comment);
        }
      }
    }
    return commentList;
  }

}
