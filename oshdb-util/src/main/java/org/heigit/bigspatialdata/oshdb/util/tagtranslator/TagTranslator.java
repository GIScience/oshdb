package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBRole;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTagOrRoleNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Easily translate your textual tags and roles to OSHDB's internal
 * representation (encoded as integers) and vice versa.
 *
 * <p>This class handles missing/not-found data in the following ways:</p>
 * <ul>
 *   <li>
 *     for (tag/role) strings that cannot be found in a keytable, the tagtranslator will generate a
 *     (temporary, internal and negative) id which can afterwards be resolved back to the input
 *     string when using the <i>same</i> tagtranslator object.
 *   </li>
 *   <li>
 *     for ids that are not found neither in the keytable nor the tagtranslator's cache, a runtime
 *     exception is thrown
 *   </li>
 * </ul>
 */
public class TagTranslator implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TagTranslator.class);
  private static final String UNABLE_TO_ACCESS_KEYTABLES = "Unable to access keytables";

  private final PreparedStatement keyIdQuery;
  private final PreparedStatement keyTxtQuery;
  private final PreparedStatement valueIdQuery;
  private final PreparedStatement valueTxtQuery;
  private final PreparedStatement roleIdQuery;
  private final PreparedStatement roleTxtQuery;

  private final ConcurrentHashMap<OSMTagKey, OSHDBTagKey> keyToInt;
  private final ConcurrentHashMap<OSHDBTagKey, OSMTagKey> keyToString;
  private final ConcurrentHashMap<OSMTag, OSHDBTag> tagToInt;
  private final ConcurrentHashMap<OSHDBTag, OSMTag> tagToString;
  private final ConcurrentHashMap<OSMRole, OSHDBRole> roleToInt;
  private final ConcurrentHashMap<OSHDBRole, OSMRole> roleToString;

  private final Connection conn;

  /**
   * A TagTranslator for a specific DB-Connection. It has its own internal cache
   * to speed up searching.
   *
   * @param conn a connection to a database (containing oshdb keytables).
   * @throws OSHDBKeytablesNotFoundException if the supplied database doesn't contain the required
   *         "keyTables" tables
   */
  public TagTranslator(Connection conn) throws OSHDBKeytablesNotFoundException {
    this.conn = conn;
    this.keyToInt = new ConcurrentHashMap<>(0);
    this.keyToString = new ConcurrentHashMap<>(0);
    this.tagToInt = new ConcurrentHashMap<>(0);
    this.tagToString = new ConcurrentHashMap<>(0);
    this.roleToInt = new ConcurrentHashMap<>(0);
    this.roleToString = new ConcurrentHashMap<>(0);

    // test connection for presence of actual "keytables" tables
    EnumSet<TableNames> keyTables =
        EnumSet.of(TableNames.E_KEY, TableNames.E_KEYVALUE, TableNames.E_ROLE);
    for (TableNames table : keyTables) {
      try (Statement testTablePresentQuery = this.conn.createStatement()) {
        testTablePresentQuery.execute("select 1 from " + table.toString() + " LIMIT 1");
      } catch (SQLException e) {
        throw new OSHDBKeytablesNotFoundException();
      }
    }

    // create prepared statements for querying tags from keytables
    try {
      keyIdQuery = conn.prepareStatement(
          "select ID from " + TableNames.E_KEY.toString() + " where KEY.TXT = ?;");
      keyTxtQuery = conn.prepareStatement(
          "select TXT from " + TableNames.E_KEY.toString() + " where KEY.ID = ?;");
      valueIdQuery = conn.prepareStatement("select k.ID as KEYID,kv.VALUEID as VALUEID"
          + " from " + TableNames.E_KEYVALUE.toString() + " kv"
          + " inner join " + TableNames.E_KEY.toString() + " k on k.ID = kv.KEYID"
          + " where k.TXT = ? and kv.TXT = ?;");
      valueTxtQuery = conn.prepareStatement("select k.TXT as KEYTXT,kv.TXT as VALUETXT"
          + " from " + TableNames.E_KEYVALUE.toString() + " kv"
          + " inner join " + TableNames.E_KEY.toString() + " k on k.ID = kv.KEYID"
          + " where k.ID = ? and kv.VALUEID = ?;");
      roleIdQuery = conn
          .prepareStatement("select ID from " + TableNames.E_ROLE.toString() + " where TXT = ?;");
      roleTxtQuery = conn
          .prepareStatement("select TXT from " + TableNames.E_ROLE.toString() + " where ID = ?;");
    } catch (SQLException e) {
      throw new OSHDBKeytablesNotFoundException();
    }
  }

  @Override
  public void close() throws SQLException {
    keyIdQuery.close();
    keyTxtQuery.close();
    valueIdQuery.close();
    valueTxtQuery.close();
    roleIdQuery.close();
    roleTxtQuery.close();
  }

  /**
   * Get oshdb's internal representation of a tag key (string).
   *
   * @param key the tag key as a string
   * @return the corresponding oshdb representation of this key
   */
  public OSHDBTagKey getOSHDBTagKeyOf(String key) {
    return getOSHDBTagKeyOf(new OSMTagKey(key));
  }

  /**
   * Get oshdb's internal representation of a tag key.
   *
   * @param key the tag key as an OSMTagKey object
   * @return the corresponding oshdb representation of this key
   */
  public OSHDBTagKey getOSHDBTagKeyOf(OSMTagKey key) {
    if (this.keyToInt.containsKey(key)) {
      return this.keyToInt.get(key);
    }
    OSHDBTagKey keyInt;
    try {
      synchronized (keyIdQuery) {
        keyIdQuery.setString(1, key.toString());
        try (ResultSet keys = keyIdQuery.executeQuery()) {
          if (!keys.next()) {
            LOG.info("Unable to find tag key {} in keytables.", key);
            keyInt = new OSHDBTagKey(getFakeId(key.toString()));
          } else {
            keyInt = new OSHDBTagKey(keys.getInt("ID"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new RuntimeException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.keyToString.put(keyInt, key);
    this.keyToInt.put(key, keyInt);
    return keyInt;
  }

  /**
   * Get a tag key's string representation from oshdb's internal data format.
   *
   * @param key the tag key (represented as an integer)
   * @return the textual representation of this tag key
   * @throws OSHDBTagOrRoleNotFoundException if the given tag key cannot be found
   */
  public OSMTagKey getOSMTagKeyOf(int key) {
    return getOSMTagKeyOf(new OSHDBTagKey(key));
  }

  /**
   * Get a tag key's string representation from oshdb's internal data format.
   *
   * @param key the tag key (as an OSHDBTagKey object)
   * @return the textual representation of this tag key
   * @throws OSHDBTagOrRoleNotFoundException if the given tag key cannot be found
   */
  public OSMTagKey getOSMTagKeyOf(OSHDBTagKey key) {
    if (this.keyToString.containsKey(key)) {
      return this.keyToString.get(key);
    }
    OSMTagKey keyString;
    try {
      synchronized (keyTxtQuery) {
        keyTxtQuery.setInt(1, key.toInt());
        try (ResultSet keys = keyTxtQuery.executeQuery()) {
          if (!keys.next()) {
            throw new OSHDBTagOrRoleNotFoundException(String.format(
                "Unable to find tag key id %d in keytables.", key.toInt()
            ));
          } else {
            keyString = new OSMTagKey(keys.getString("TXT"));
          }
        }
        this.keyToInt.put(keyString, key);
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new RuntimeException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.keyToString.put(key, keyString);
    return keyString;
  }

  /**
   * Get oshdb's internal representation of a tag (key-value string pair).
   *
   * @param key the (string) key of the tag
   * @param value the (string) value of the tag
   * @return the corresponding oshdb representation of this tag
   */
  public OSHDBTag getOSHDBTagOf(String key, String value) {
    return this.getOSHDBTagOf(new OSMTag(key, value));
  }

  /**
   * Get oshdb's internal representation of a tag (key-value pair).
   *
   * @param tag a key-value pair as an OSMTag object
   * @return the corresponding oshdb representation of this tag
   */
  public OSHDBTag getOSHDBTagOf(OSMTag tag) {
    // check if Key and Value are in cache
    if (this.tagToInt.containsKey(tag)) {
      return this.tagToInt.get(tag);
    }
    OSHDBTag tagInt;
    // key or value is not in cache so let's go toInt them
    try {
      synchronized (valueIdQuery) {
        valueIdQuery.setString(1, tag.getKey());
        valueIdQuery.setString(2, tag.getValue());
        try (ResultSet values = valueIdQuery.executeQuery()) {
          if (!values.next()) {
            LOG.info("Unable to find tag {}={} in keytables.", tag.getKey(), tag.getValue());
            tagInt = new OSHDBTag(this.getOSHDBTagKeyOf(tag.getKey()).toInt(), getFakeId(tag.getValue()));
          } else {
            tagInt = new OSHDBTag(values.getInt("KEYID"), values.getInt("VALUEID"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new RuntimeException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.tagToString.put(tagInt, tag);
    this.tagToInt.put(tag, tagInt);
    return tagInt;
  }

  /**
   * Get a tag's string representation from oshdb's internal data format.
   *
   * @param key the key of the tag (represented as an integer)
   * @param value the value of the tag (represented as an integer)
   * @return the textual representation of this tag
   * @throws OSHDBTagOrRoleNotFoundException if the given tag cannot be found
   */
  public OSMTag getOSMTagOf(int key, int value) {
    return this.getOSMTagOf(new OSHDBTag(key, value));
  }

  /**
   * Get a tag's string representation from oshdb's internal data format.
   *
   * @param tag the tag (as an OSHDBTag object)
   * @return the textual representation of this tag
   * @throws OSHDBTagOrRoleNotFoundException if the given tag cannot be found
   */
  public OSMTag getOSMTagOf(OSHDBTag tag) {
    // check if Key and Value are in cache
    if (this.tagToString.containsKey(tag)) {
      return this.tagToString.get(tag);
    }
    OSMTag tagString;

    // key or value is not in cache so let's go toInt them
    try {
      synchronized (valueTxtQuery) {
        valueTxtQuery.setInt(1, tag.getKey());
        valueTxtQuery.setInt(2, tag.getValue());
        try (ResultSet values = valueTxtQuery.executeQuery()) {
          if (!values.next()) {
            throw new OSHDBTagOrRoleNotFoundException(String.format(
                "Unable to find tag id %d=%d in keytables.",
                tag.getKey(), tag.getValue()
            ));
          } else {
            tagString = new OSMTag(values.getString("KEYTXT"), values.getString("VALUETXT"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new RuntimeException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    // put it in caches
    this.tagToInt.put(tagString, tag);
    this.tagToString.put(tag, tagString);
    return tagString;
  }

  /**
   * Get oshdb's internal representation of a role (string).
   *
   * @param role the role string to fetch
   * @return the corresponding oshdb representation of this role
   */
  public OSHDBRole getOSHDBRoleOf(String role) {
    return this.getOSHDBRoleOf(new OSMRole(role));
  }

  /**
   * Get oshdb's internal representation of a role.
   *
   * @param role the role to fetch as an OSMRole object
   * @return the corresponding oshdb representation of this role
   */
  public OSHDBRole getOSHDBRoleOf(OSMRole role) {
    if (this.roleToInt.containsKey(role)) {
      return this.roleToInt.get(role);
    }
    OSHDBRole roleInt;
    try {
      synchronized (roleIdQuery) {
        roleIdQuery.setString(1, role.toString());
        try (ResultSet roles = roleIdQuery.executeQuery()) {
          if (!roles.next()) {
            LOG.info("Unable to find role {} in keytables.", role);
            roleInt = new OSHDBRole(getFakeId(role.toString()));
          } else {
            roleInt = new OSHDBRole(roles.getInt("ID"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new RuntimeException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.roleToString.put(roleInt, role);
    this.roleToInt.put(role, roleInt);
    return roleInt;
  }

  /**
   * Get a role's string representation from oshdb's internal data format.
   *
   * @param role the role ID (represented as an integer)
   * @return the textual representation of this role
   * @throws OSHDBTagOrRoleNotFoundException if the given role cannot be found
   */
  public OSMRole getOSMRoleOf(int role) {
    return this.getOSMRoleOf(new OSHDBRole(role));
  }

  /**
   * Get a role's string representation from oshdb's internal data format.
   *
   * @param role the role ID (as an OSHDBRole object)
   * @return the textual representation of this role
   * @throws OSHDBTagOrRoleNotFoundException if the given role cannot be found
   */
  public OSMRole getOSMRoleOf(OSHDBRole role) {
    if (this.roleToString.containsKey(role)) {
      return this.roleToString.get(role);
    }
    OSMRole roleString;
    try {
      synchronized (roleTxtQuery) {
        roleTxtQuery.setInt(1, role.toInt());
        try (ResultSet roles = roleTxtQuery.executeQuery()) {
          if (!roles.next()) {
            throw new OSHDBTagOrRoleNotFoundException(String.format(
                "Unable to find role id %d in keytables.", role.toInt()
            ));
          } else {
            roleString = new OSMRole(roles.getString("TXT"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new RuntimeException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.roleToInt.put(roleString, role);
    this.roleToString.put(role, roleString);
    return roleString;
  }

  private int getFakeId(String s) {
    return -(s.hashCode() & 0x7fffffff);
  }

  public Connection getConnection() {
    return conn;
  }
}
