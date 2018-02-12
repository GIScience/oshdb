package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

import org.heigit.bigspatialdata.oshdb.TableNames;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
  public static final int UNKNOWN_USER_ID = -1;
  private static final String UNKNOWN_USER_STR = "<unknown user name>";

  private final Map<String, Integer> keyToInt;
  private final Map<Integer, String> keyToString;
  private final Map<Pair<String,String>, Pair<Integer,Integer>> tagToInt;
  private final Map<Pair<Integer,Integer>, Pair<String,String>> tagToString;
  private final Map<String, Integer> roleToInt;
  private final Map<Integer, String> roleToString;
  private final Map<String, Integer> userToInt;
  private final Map<Integer, String> userToString;

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
    this.userToInt = new ConcurrentHashMap<>(0);
    this.userToString = new ConcurrentHashMap<>(0);

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
    keyToString.put(UNKNOWN_TAG_KEY_ID, UNKNOWN_TAG_KEY_STR);
    keyToInt.put(UNKNOWN_TAG_KEY_STR, UNKNOWN_TAG_KEY_ID);
    tagToString.put(
        new ImmutablePair<>(UNKNOWN_TAG_KEY_ID, UNKNOWN_TAG_VALUE_ID),
        new ImmutablePair<>(UNKNOWN_TAG_KEY_STR, UNKNOWN_TAG_VALUE_STR));
    tagToInt.put(
        new ImmutablePair<>(UNKNOWN_TAG_KEY_STR, UNKNOWN_TAG_VALUE_STR),
        new ImmutablePair<>(UNKNOWN_TAG_KEY_ID, UNKNOWN_TAG_VALUE_ID));
    roleToString.put(UNKNOWN_ROLE_ID, UNKNOWN_ROLE_STR);
    //roleToInt.put(UNKNOWN_ROLE_STR, UNKNOWN_ROLE_ID);
    //userToString.put(UNKNOWN_USER_ID, UNKNOWN_USER_STR);
    //userToInt.put(UNKNOWN_USER_STR, UNKNOWN_USER_ID);
  }

  /**
   * Get ID (integer) of a tag key.
   *
   * @param key the tag key
   * @return the corresponding integer ID of this key, or null if it cannot be found
   */
  public Integer key2Int(String key) {
    if (this.tagToInt.containsKey(key)) {
      return this.tagToInt.get(key).getKey();
    }
    Integer keyInt;
    try (PreparedStatement keystmt = this.conn.prepareStatement(
        "select ID from " + TableNames.E_KEY.toString() + " where KEY.TXT = ?;")) {
      keystmt.setString(1, key);
      ResultSet keys = keystmt.executeQuery();
      keys.next();
      keyInt = keys.getInt("ID");
      this.keyToString.put(keyInt, key);
    } catch (SQLException ex) {
      LOG.info("Unable to find tag key \"{}\" in keytables.", key);
      keyInt = UNKNOWN_TAG_KEY_ID;
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
  public String key2String(Integer key) {
    if (this.keyToString.containsKey(key)) {
      return this.keyToString.get(key);
    }
    String keyString;
    try (PreparedStatement keystmt =
        this.conn.prepareStatement("select TXT from KEY where KEY.ID = ?;")) {
      keystmt.setInt(1, key);
      ResultSet keys = keystmt.executeQuery();
      keys.next();
      keyString = keys.getString("TXT");
      this.keyToInt.put(keyString, key);
    } catch (SQLException ex) {
      LOG.warn("Unable to find tag key id \"{}\" in keytables.", key);
      keyString = UNKNOWN_TAG_KEY_STR;
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
  public Pair<Integer, Integer> tag2Int(String key, String value) {
    return this.tag2Int(new ImmutablePair<>(key, value));
  }

  /**
   * Get the IDs of a tag (key=value pair).
   *
   * @param tag a key-value pair
   * @return the corresponding key-value ID (integer) Pair, or null if it cannot be found
   */
  public Pair<Integer, Integer> tag2Int(Pair<String, String> tag) {
    // check if Key and Value are in cache
    if (this.tagToInt.containsKey(tag)) {
      return this.tagToInt.get(tag);
    }
    Pair<Integer, Integer> tagInt;
    // key or value is not in cache so let's go get them
    try (PreparedStatement valstmt =
        this.conn.prepareStatement("select k.ID as KEYID,kv.VALUEID as VALUEID "
            + "from " + TableNames.E_KEYVALUE.toString() + " kv "
            + "inner join " + TableNames.E_KEY.toString() + " k on k.ID = kv.KEYID "
            + "WHERE k.TXT = ? and kv.TXT = ?;")) {
      valstmt.setString(1, tag.getKey());
      valstmt.setString(2, tag.getValue());
      ResultSet values = valstmt.executeQuery();
      values.next();
      tagInt = new ImmutablePair<>(values.getInt("KEYID"), values.getInt("VALUEID"));
      this.tagToString.put(tagInt, tag);
    } catch (SQLException ex) {
      LOG.info("Unable to find tag \"{}\"=\"{}\" in keytables.", tag.getKey(), tag.getValue());
      tagInt = new ImmutablePair<>(this.key2Int(tag.getKey()), UNKNOWN_TAG_VALUE_ID);
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
  public Pair<String, String> tag2String(int key, int value) {
    return this.tag2String(new ImmutablePair<>(key, value));
  }

  /**
   * Get a tags text from a key-value ID pair.
   *
   * @param tag the key-value pair (of integers) to fetch
   * @return the key-value pair (of strings) of the corresponding tag, or null if it cannot be found
   */
  public Pair<String, String> tag2String(Pair<Integer, Integer> tag) {
    // check if Key and Value are in cache
    if (this.tagToString.containsKey(tag)) {
      return this.tagToString.get(tag);
    }
    Pair<String, String> tagString;

    // key or value is not in cache so let's go get them
    try (PreparedStatement valstmt =
        this.conn.prepareStatement("select k.TXT as KEYTXT,kv.TXT as VALUETXT from "
            + TableNames.E_KEYVALUE.toString() + " kv inner join " + TableNames.E_KEY.toString()
            + " k on k.ID = kv.KEYID WHERE k.ID = ? and kv.VALUEID = ?;")) {
      valstmt.setInt(1, tag.getKey());
      valstmt.setInt(2, tag.getValue());
      ResultSet values = valstmt.executeQuery();
      values.next();
      tagString = new ImmutablePair<>(values.getString("KEYTXT"), values.getString("VALUETXT"));
      this.tagToInt.put(tagString, tag);
    } catch (SQLException ex) {
      LOG.warn("Unable to find tag id \"{}\"=\"{}\" in keytables.", tag.getKey(), tag.getValue());
      tagString = new ImmutablePair<>(this.key2String(tag.getKey()), UNKNOWN_TAG_VALUE_STR);
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
  public Integer role2Int(String role) {
    if (this.roleToInt.containsKey(role)) {
      return this.roleToInt.get(role);
    }
    Integer roleInt;
    try (PreparedStatement rolestmt = conn
        .prepareStatement("select ID from " + TableNames.E_ROLE.toString() + " WHERE txt = ?;")) {
      rolestmt.setString(1, role);
      ResultSet roles = rolestmt.executeQuery();
      roles.next();
      roleInt = roles.getInt("ID");
      this.roleToString.put(roleInt, role);
    } catch (SQLException ex) {
      LOG.info("Unable to find role \"{}\" in keytables.", role);
      roleInt = UNKNOWN_ROLE_ID;
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
  public String role2String(Integer role) {
    if (this.roleToString.containsKey(role)) {
      return this.roleToString.get(role);
    }
    String roleString;
    try (PreparedStatement Rolestmt = conn
        .prepareStatement("select TXT from " + TableNames.E_ROLE.toString() + " WHERE ID = ?;")) {
      Rolestmt.setInt(1, role);
      ResultSet Roles = Rolestmt.executeQuery();
      Roles.next();
      roleString = Roles.getString("TXT");
      this.roleToInt.put(roleString, role);
    } catch (SQLException ex) {
      LOG.info("Unable to find role id \"{}\" in keytables.", role);
      roleString = UNKNOWN_ROLE_STR;
    }
    this.roleToString.put(role, roleString);
    return roleString;
  }
}
