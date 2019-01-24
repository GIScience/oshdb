package org.heigit.bigspatialdata.oshdb.util.userdetails;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enriches a OSMUserId with a name-string.
 */
public class UserNameResolver {

  private final Connection conn;
  private final Map<Long, OSMUser> users;

  /**
   * This class works as a cache (like
   * {@link org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator}). Attention: The
   * UserDatabase is minutely updated but the cache isn't. So subsequent calls to this Class for the
   * same ID will always return the same result. If you want to update the cache you will have to
   * create a new instance.
   *
   * @param conn The connection to the ohsome-whosthat postgres-database.
   */
  public UserNameResolver(Connection conn) {
    this.users = new ConcurrentHashMap<>(0);
    this.conn = conn;
  }

  /**
   * Get an OSMUser by his/her ID.
   *
   * @param userId ID of a user.
   * @return An OSMUser.
   * @throws SQLException
   */
  public OSMUser getUser(long userId) throws SQLException {
    if (this.users.containsKey(userId)) {
      return this.users.get(userId);
    }
    ArrayList<String> userNames = new ArrayList<>(0);
    try (PreparedStatement prepareStatement = this.conn.prepareStatement(
        "SELECT user_name "
        + "FROM whosthat "
        + "ORDER BY date_last DESC"
        + "WHERE user_id = ? ;")) {
      prepareStatement.setLong(1, userId);
      ResultSet resultSet = prepareStatement.executeQuery();

      while (resultSet.next()) {
        userNames.add(resultSet.getString("user_name"));
      }
    }
    OSMUser osmUser = new OSMUser(userId, userNames);
    this.users.put(userId, osmUser);
    return osmUser;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 53 * hash + Objects.hashCode(this.conn);
    hash = 53 * hash + Objects.hashCode(this.users);
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
    final UserNameResolver other = (UserNameResolver) obj;
    if (!Objects.equals(this.conn, other.conn)) {
      return false;
    }
    return Objects.equals(this.users, other.users);
  }

}
