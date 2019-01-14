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
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.slf4j.LoggerFactory;

public class ChangesetEnricher {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TagTranslator.class);

  private final Connection conn;
  private final Map<Long, OSMChangeset> changests;

  public ChangesetEnricher(Connection conn) {
    this.changests = new ConcurrentHashMap<>(0);
    this.conn = conn;
  }

  /**
   * Get latest update Timestamp of Changeset-Database (may be null if unknown).
   *
   * @return
   * @throws SQLException
   */
  public OSHDBTimestamp getValidUntil() throws SQLException {

    Timestamp timestamp;
    try (PreparedStatement prepareStatement = this.conn.prepareStatement("SELECT last_timestamp FROM osm_changeset_state")) {
      ResultSet executeQuery = prepareStatement.executeQuery();
      executeQuery.next();
      timestamp = executeQuery.getTimestamp(1);
    }
    if (timestamp == null) {
      return null;
    }
    return new OSHDBTimestamp(timestamp);
  }

  /**
   * Get a Changeset with comments. May be null if changesetId not in Database.
   *
   * @param changesetId
   * @return
   */
  public OSMChangeset getChangeset(long changesetId) {
    if (this.changests.containsKey(changesetId)) {
      return this.changests.get(changesetId);
    }
    OSMChangeset changeset;
    try (PreparedStatement prepareStatement = this.conn.prepareStatement(
        "SELECT user_id,created_at,min_lat,max_lat,min_lon,max_lon,closed_at,open,num_changes,user_name,tags,comment_changeset_id,comment_user_id,comment_user_name,comment_date,comment_text "
        + "FROM osm_changeset "
        + "LEFT JOIN osm_changeset_comment "
        + "ON osm_changeset.id=osm_changeset_comment.comment_changeset_id "
        + "WHERE id = ? ;")) {
      prepareStatement.setLong(1, changesetId);
      ResultSet resultSet = prepareStatement.executeQuery();
      resultSet.next();

      Long user_id = resultSet.getLong("user_id");

      OSHDBTimestamp created_at;
      try {
        created_at = new OSHDBTimestamp(resultSet.getTimestamp("created_at"));
      } catch (NullPointerException ex) {
        created_at = null;
      }

      OSHDBBoundingBox bbx;
      try {
        bbx = new OSHDBBoundingBox(resultSet.getDouble("min_lon"), resultSet.getDouble("min_lat"), resultSet.getDouble("max_lon"), resultSet.getDouble("max_lat"));
      } catch (NullPointerException ex) {
        bbx = null;
      }

      OSHDBTimestamp closed_at;
      try {
        closed_at = new OSHDBTimestamp(resultSet.getTimestamp("closed_at"));
      } catch (NullPointerException ex) {
        closed_at = null;
      }

      Boolean open = resultSet.getBoolean("open");

      Integer num_changes = resultSet.getInt("num_changes");

      String user_name = resultSet.getString("user_name");

      @SuppressWarnings("unchecked")
      Map<String, String> tags = (Map<String, String>) resultSet.getObject("tags");

      resultSet.getLong("comment_changeset_id");
      if (resultSet.wasNull()) {
        changeset = new OSMChangeset(changesetId, user_id, created_at, bbx, closed_at, open, num_changes, user_name, tags, null);
      } else {
        List<OSMChangesetComment> commentList = new ArrayList<>(1);
        while (!resultSet.isAfterLast()) {
          Long comment_changeset_id = resultSet.getLong("comment_changeset_id");

          Long comment_user_id = resultSet.getLong("comment_user_id");

          String comment_user_name = resultSet.getString("comment_user_name");

          OSHDBTimestamp comment_date;
          try {
            comment_date = new OSHDBTimestamp(resultSet.getTimestamp("comment_date"));
          } catch (NullPointerException ex) {
            comment_date = null;
          }

          String comment_text = resultSet.getString("comment_text");

          OSMChangesetComment comment = new OSMChangesetComment(comment_changeset_id, comment_user_id, comment_user_name, comment_date, comment_text);
          commentList.add(comment);
          resultSet.next();
        }
        changeset = new OSMChangeset(changesetId, user_id, created_at, bbx, closed_at, open, num_changes, user_name, tags, commentList);
      }

    } catch (SQLException ex) {
      LOG.info("Unable to find changeset-id {} in database.", changesetId);
      changeset = null;
    }
    this.changests.put(changesetId, changeset);
    return changeset;

  }

}
