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
  private final PreparedStatement userPs;

  /**
   * This class works as a cache (like
   * {@link org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator}). The user is first
   * searched in the local cache. If not present the sql-database is queried and results are added
   * to the cache. By using a ConcurrentHashMap internally, this class is threadsafe. The idea is to
   * have one instance (cache) in your program to gain maximum speed and prevent overuse of the
   * SQL-db. Attention: The UserDatabase is minutely updated but the cache isn't. So subsequent
   * calls to this Class for the same ID will always return the same result. If you want to update
   * the cache you will have to create a new instance.
   *
   * @param conn The connection to the ohsome-whosthat postgres-database.
   */
  public UserNameResolver(Connection conn) throws SQLException {
    this.users = new ConcurrentHashMap<>(0);
    this.conn = conn;
    this.userPs = this.conn.prepareStatement(
        "SELECT user_name "
        + "FROM whosthat "
        + "ORDER BY date_last DESC"
        + "WHERE user_id = ? ;");
  }

  /**
   * Get an OSMUser by his/her ID.
   *
   * @param userId ID of a user.
   * @return An OSMUser.
   * @throws SQLException If something went wrong when querying the db
   */
  public OSMUser getUser(long userId) throws SQLException {
    if (this.users.containsKey(userId)) {
      return this.users.get(userId);
    }
    ArrayList<String> userNames = new ArrayList<>(0);

    this.userPs.setLong(1, userId);
    try (ResultSet resultSet = this.userPs.executeQuery()) {
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
