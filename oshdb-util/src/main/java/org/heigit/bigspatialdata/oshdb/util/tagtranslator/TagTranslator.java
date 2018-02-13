package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

import org.heigit.bigspatialdata.oshdb.TableNames;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBRole;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Easily translate your String-Keys or Values to OSHDb integer and vice versa.
 *
 */
public class TagTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(TagTranslator.class);

  public static final int UNKNOWN_TAG_KEY_ID = -1;
  private static final String UNKNOWN_TAG_KEY_STR = "<unknown tag key>";
  public static final int UNKNOWN_TAG_VALUE_ID = -1;
  private static final String UNKNOWN_TAG_VALUE_STR = "<unknown tag value>";
  public static final int UNKNOWN_ROLE_ID = -1;
  private static final String UNKNOWN_ROLE_STR = "<unknown role>";

  private final Map<OSMTagKey, OSHDBTagKey> keyToInt;
  private final Map<OSHDBTagKey, OSMTagKey> keyToString;
  private final Map<OSMTag, OSHDBTag> tagToInt;
  private final Map<OSHDBTag, OSMTag> tagToString;
  private final Map<OSMRole, OSHDBRole> roleToInt;
  private final Map<OSHDBRole, OSMRole> roleToString;

  private final Connection conn;

  /**
   * A TagTranslator for a specific DB-Connection. Keep it safe and feed it from time to time! It
   * has its own lazy cache to speed up searching.
   *
   * @param conn a connection to a database. For now only H2 is supported
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
      try {
        this.conn.prepareStatement("select 1 from " + table.toString() + " LIMIT 1").execute();
      } catch (SQLException e) {
        throw new OSHDBKeytablesNotFoundException();
      }
    }

    // pre-populate caches with id<->string pairs for "no-data" cases
    keyToString.put(new OSHDBTagKey(UNKNOWN_TAG_KEY_ID), new OSMTagKey(UNKNOWN_TAG_KEY_STR));
    keyToInt.put(new OSMTagKey(UNKNOWN_TAG_KEY_STR), new OSHDBTagKey(UNKNOWN_TAG_KEY_ID));
    tagToString.put(
        new OSHDBTag(UNKNOWN_TAG_KEY_ID, UNKNOWN_TAG_VALUE_ID),
        new OSMTag(UNKNOWN_TAG_KEY_STR, UNKNOWN_TAG_VALUE_STR));
    tagToInt.put(
        new OSMTag(UNKNOWN_TAG_KEY_STR, UNKNOWN_TAG_VALUE_STR),
        new OSHDBTag(UNKNOWN_TAG_KEY_ID, UNKNOWN_TAG_VALUE_ID));
    roleToString.put(new OSHDBRole(UNKNOWN_ROLE_ID), new OSMRole(UNKNOWN_ROLE_STR));
    roleToInt.put(new OSMRole(UNKNOWN_ROLE_STR), new OSHDBRole(UNKNOWN_ROLE_ID));
  }

  /**
   * Get ID (integer) of a tag key.
   *
   * @param key the tag key
   * @return the corresponding integer ID of this key, or null if it cannot be found
   */
  public OSHDBTagKey oshdbTagKeyOf(String key) {
    return oshdbTagKeyOf(new OSMTagKey(key));
  }

  /**
   * Get ID (integer) of a tag key.
   *
   * @param key the tag key
   * @return the corresponding integer ID of this key, or null if it cannot be found
   */
  public OSHDBTagKey oshdbTagKeyOf(OSMTagKey key) {
    if (this.keyToInt.containsKey(key)) {
      return this.keyToInt.get(key);
    }
    OSHDBTagKey keyInt;
    try (PreparedStatement keystmt = this.conn.prepareStatement(
        "select ID from " + TableNames.E_KEY.toString() + " where KEY.TXT = ?;")) {
      keystmt.setString(1, key.toString());
      ResultSet keys = keystmt.executeQuery();
      keys.next();
      keyInt = new OSHDBTagKey(keys.getInt("ID"));
      this.keyToString.put(keyInt, key);
    } catch (SQLException ex) {
      LOG.info("Unable to find tag key \"{}\" in keytables.", key);
      keyInt = new OSHDBTagKey(UNKNOWN_TAG_KEY_ID);
    }
    this.keyToInt.put(key, keyInt);
    return keyInt;
  }

  /**
   * Get a tag key text (string) of a single key ID.
   *
   * @param key the ID (integer) of the tag key to fetch
   * @return the text (string) of the corresponding tag key, or null if it cannot be found
   */
  public OSMTagKey osmTagKeyOf(int key) {
    return osmTagKeyOf(new OSHDBTagKey(key));
  }

  /**
   * Get a tag key text (string) of a single key ID.
   *
   * @param key the ID (integer) of the tag key to fetch
   * @return the text (string) of the corresponding tag key, or null if it cannot be found
   */
  public OSMTagKey osmTagKeyOf(OSHDBTagKey key) {
    if (this.keyToString.containsKey(key)) {
      return this.keyToString.get(key);
    }
    OSMTagKey keyString;
    try (PreparedStatement keystmt =
        this.conn.prepareStatement("select TXT from KEY where KEY.ID = ?;")) {
      keystmt.setInt(1, key.toInt());
      ResultSet keys = keystmt.executeQuery();
      keys.next();
      keyString = new OSMTagKey(keys.getString("TXT"));
      this.keyToInt.put(keyString, key);
    } catch (SQLException ex) {
      LOG.warn("Unable to find tag key id \"{}\" in keytables.", key);
      keyString = new OSMTagKey(UNKNOWN_TAG_KEY_STR);
    }
    this.keyToString.put(key, keyString);
    return keyString;
  }

  /**
   * Get the IDs of a tag (key=value pair).
   *
   * @param key the key of the tag
   * @param value the value of the tag
   * @return the corresponding key-value ID (integer) Pair, or null if it cannot be found
   */
  public OSHDBTag oshdbTagOf(String key, String value) {
    return this.oshdbTagOf(new OSMTag(key, value));
  }

  /**
   * Get the IDs of a tag (key=value pair).
   *
   * @param tag a key-value pair
   * @return the corresponding key-value ID (integer) Pair, or null if it cannot be found
   */
  public OSHDBTag oshdbTagOf(OSMTag tag) {
    // check if Key and Value are in cache
    if (this.tagToInt.containsKey(tag)) {
      return this.tagToInt.get(tag);
    }
    OSHDBTag tagInt;
    // key or value is not in cache so let's go toInt them
    try (PreparedStatement valstmt =
        this.conn.prepareStatement("select k.ID as KEYID,kv.VALUEID as VALUEID "
            + "from " + TableNames.E_KEYVALUE.toString() + " kv "
            + "inner join " + TableNames.E_KEY.toString() + " k on k.ID = kv.KEYID "
            + "WHERE k.TXT = ? and kv.TXT = ?;")) {
      valstmt.setString(1, tag.getKey());
      valstmt.setString(2, tag.getValue());
      ResultSet values = valstmt.executeQuery();
      values.next();
      tagInt = new OSHDBTag(values.getInt("KEYID"), values.getInt("VALUEID"));
      this.tagToString.put(tagInt, tag);
    } catch (SQLException ex) {
      LOG.info("Unable to find tag \"{}\"=\"{}\" in keytables.", tag.getKey(), tag.getValue());
      tagInt = new OSHDBTag(this.oshdbTagKeyOf(tag.getKey()).toInt(), UNKNOWN_TAG_VALUE_ID);
    }
    this.tagToInt.put(tag, tagInt);
    return tagInt;
  }

  /**
   * Get a tags text from a key-value ID pair.
   *
   * @param key the key of the tag (integer) to fetch
   * @param value the value of the tag (integer) to fetch
   * @return the key-value pair (of strings) of the corresponding tag, or null if it cannot be found
   */
  public OSMTag osmTagOf(int key, int value) {
    return this.osmTagOf(new OSHDBTag(key, value));
  }

  /**
   * Get a tags text from a key-value ID pair.
   *
   * @param tag the key-value pair (of integers) to fetch
   * @return the key-value pair (of strings) of the corresponding tag, or null if it cannot be found
   */
  public OSMTag osmTagOf(OSHDBTag tag) {
    // check if Key and Value are in cache
    if (this.tagToString.containsKey(tag)) {
      return this.tagToString.get(tag);
    }
    OSMTag tagString;

    // key or value is not in cache so let's go toInt them
    try (PreparedStatement valstmt =
        this.conn.prepareStatement("select k.TXT as KEYTXT,kv.TXT as VALUETXT from "
            + TableNames.E_KEYVALUE.toString() + " kv inner join " + TableNames.E_KEY.toString()
            + " k on k.ID = kv.KEYID WHERE k.ID = ? and kv.VALUEID = ?;")) {
      valstmt.setInt(1, tag.getKey());
      valstmt.setInt(2, tag.getValue());
      ResultSet values = valstmt.executeQuery();
      values.next();
      tagString = new OSMTag(values.getString("KEYTXT"), values.getString("VALUETXT"));
      this.tagToInt.put(tagString, tag);
    } catch (SQLException ex) {
      LOG.warn("Unable to find tag id \"{}\"=\"{}\" in keytables.", tag.getKey(), tag.getValue());
      tagString = new OSMTag(this.osmTagKeyOf(tag.getKey()).toString(), UNKNOWN_TAG_VALUE_STR);
    }
    // put it in caches
    this.tagToString.put(tag, tagString);
    return tagString;
  }

  /**
   * Get the ID for a role.
   *
   * @param role the role string to fetch
   * @return the integer ID of this role, or null if it cannot be found
   */
  public OSHDBRole oshdbRoleOf(String role) {
    return this.oshdbRoleOf(new OSMRole(role));
  }

  /**
   * Get the ID for a role.
   *
   * @param role the role string to fetch
   * @return the integer ID of this role, or null if it cannot be found
   */
  public OSHDBRole oshdbRoleOf(OSMRole role) {
    if (this.roleToInt.containsKey(role)) {
      return this.roleToInt.get(role);
    }
    OSHDBRole roleInt;
    try (PreparedStatement rolestmt = conn
        .prepareStatement("select ID from " + TableNames.E_ROLE.toString() + " WHERE txt = ?;")) {
      rolestmt.setString(1, role.toString());
      ResultSet roles = rolestmt.executeQuery();
      roles.next();
      roleInt = new OSHDBRole(roles.getInt("ID"));
      this.roleToString.put(roleInt, role);
    } catch (SQLException ex) {
      LOG.info("Unable to find role \"{}\" in keytables.", role);
      roleInt = new OSHDBRole(UNKNOWN_ROLE_ID);
    }
    this.roleToInt.put(role, roleInt);
    return roleInt;
  }

  /**
   * Get the string for a role ID.
   *
   * @param role the role ID (integer) to fetch
   * @return the role's text (string), or null if it cannot be found
   */
  public OSMRole osmRoleOf(int role) {
    return this.osmRoleOf(new OSHDBRole(role));
  }

  /**
   * Get the string for a role ID.
   *
   * @param role the role ID (integer) to fetch
   * @return the role's text (string), or null if it cannot be found
   */
  public OSMRole osmRoleOf(OSHDBRole role) {
    if (this.roleToString.containsKey(role)) {
      return this.roleToString.get(role);
    }
    OSMRole roleString;
    try (PreparedStatement Rolestmt = conn
        .prepareStatement("select TXT from " + TableNames.E_ROLE.toString() + " WHERE ID = ?;")) {
      Rolestmt.setInt(1, role.toInt());
      ResultSet Roles = Rolestmt.executeQuery();
      Roles.next();
      roleString = new OSMRole(Roles.getString("TXT"));
      this.roleToInt.put(roleString, role);
    } catch (SQLException ex) {
      LOG.info("Unable to find role id \"{}\" in keytables.", role);
      roleString = new OSMRole(UNKNOWN_ROLE_STR);
    }
    this.roleToString.put(role, roleString);
    return roleString;
  }
}
