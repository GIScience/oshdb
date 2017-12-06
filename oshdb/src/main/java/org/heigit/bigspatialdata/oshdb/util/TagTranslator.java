package org.heigit.bigspatialdata.oshdb.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Easily translate your String-Keys or Values to OSHDb integer and vice versa.
 *
 */
public class TagTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(TagTranslator.class);

  private final Connection conn;
  private final Map<String, Pair<Integer, Map<String, Integer>>> tagToInt;
  private final Map<Integer, Pair<String, Map<Integer, String>>> tagToString;
  private final Map<String, Integer> roleToInt;
  private final Map<Integer, String> roleToString;
  private final Map<String, Integer> userToInt;
  private final Map<Integer, String> userToString;

  /**
   * A TagTranslator for a specific DB-Connection. Keep it safe and feed it from
   * time to time! It has its own lazy cache to speed up searching.
   *
   * @param conn a connection to a database. For now only H2 is supported
   * @throws OSHDBKeytablesNotFoundException if the supplied database doesn't contain the required "keyTables" tables
   */
  public TagTranslator(Connection conn) throws OSHDBKeytablesNotFoundException {
    this.conn = conn;
    this.tagToInt = new ConcurrentHashMap<>(0);
    this.tagToString = new ConcurrentHashMap<>(0);
    this.roleToInt = new ConcurrentHashMap<>(0);
    this.roleToString = new ConcurrentHashMap<>(0);
    this.userToInt = new ConcurrentHashMap<>(0);
    this.userToString = new ConcurrentHashMap<>(0);

    // test connection for presence of actual "keytables" tables
    EnumSet<TableNames> keyTables = EnumSet.of(
        TableNames.E_KEY,
        TableNames.E_KEYVALUE,
        TableNames.E_ROLE
    );
    for (TableNames table : keyTables) {
      try {
        this.conn.prepareStatement("select 1 from " + table.toString() + " LIMIT 1").execute();
      } catch (SQLException e) {
        throw new OSHDBKeytablesNotFoundException();
      }
    }
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
    String keyString = tag.getKey();
    Integer keyInt = null;
    String valueString = tag.getValue();
    Integer valInt = null;

    //check if Key and Value are in cache
    if (this.tagToInt.containsKey(keyString) && this.tagToInt.get(keyString).getValue().containsKey(valueString)) {
      return new ImmutablePair<>(this.tagToInt.get(keyString).getKey(), this.tagToInt.get(keyString).getValue().get(valueString));
    }

    //key or value is not in cache so let's go get them
    try (PreparedStatement valstmt = this.conn.prepareStatement("select k.ID as KEYID,kv.VALUEID as VALUEID from " + TableNames.E_KEYVALUE.toString() + " kv inner join " + TableNames.E_KEY.toString() + " k on k.ID = kv.KEYID WHERE k.TXT = ? and kv.TXT = ?;")) {
      valstmt.setString(1, keyString);
      valstmt.setString(2, valueString);
      ResultSet values = valstmt.executeQuery();
      values.next();
      keyInt = values.getInt("KEYID");
      valInt = values.getInt("VALUEID");

      //put it in caches
      Map<String, Integer> valResultString;
      Map<Integer, String> valResultInt;
      if (this.tagToInt.containsKey(keyString)) {
        valResultString = this.tagToInt.get(keyString).getValue();
        valResultInt = this.tagToString.get(keyInt).getValue();
      } else {
        valResultString = new ConcurrentHashMap<>(1);
        valResultInt = new ConcurrentHashMap<>(1);
      }
      valResultString.put(valueString, valInt);
      valResultInt.put(valInt, valueString);
      this.tagToInt.put(keyString, new ImmutablePair<>(keyInt, valResultString));
      this.tagToString.put(keyInt, new ImmutablePair<>(keyString, valResultInt));
    } catch (SQLException ex) {
      LOG.info("Unable to find tag \"{}\"=\"{}\" in keytables.", tag.getKey(), tag.getValue());
    }

    return new ImmutablePair<>(keyInt, valInt);

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
    Integer keyInt = null;
    try (PreparedStatement keystmt = this.conn.prepareStatement("select ID from " + TableNames.E_KEY.toString() + " where KEY.TXT = ?;")) {
      keystmt.setString(1, key);
      ResultSet keys = keystmt.executeQuery();
      keys.next();
      keyInt = keys.getInt("ID");
      this.tagToInt.put(key, new ImmutablePair<>(keyInt, new ConcurrentHashMap<>(0)));
      this.tagToString.put(keyInt, new ImmutablePair<>(key, new ConcurrentHashMap<>(0)));

    } catch (SQLException ex) {
      LOG.info("Unable to find tag key \"{}\" in keytables.", key);
    }
    return keyInt;
  }

  /**
   * Get a tag key text (string) of a single key ID.
   *
   * @param key the ID (integer) of the tag key to fetch
   * @return the text (string) of the corresponding tag key, or null if it cannot be found
   */
  public String key2String(Integer key) {
    if (this.tagToString.containsKey(key)) {
      return this.tagToString.get(key).getKey();
    }
    String keyString = null;
    try (PreparedStatement keystmt = this.conn.prepareStatement("select TXT from KEY where KEY.ID = ?;")) {
      keystmt.setInt(1, key);
      ResultSet keys = keystmt.executeQuery();
      keys.next();
      keyString = keys.getString("TXT");
      this.tagToString.put(key, new ImmutablePair<>(keyString, new ConcurrentHashMap<>(0)));
      this.tagToInt.put(keyString, new ImmutablePair<>(key, new ConcurrentHashMap<>(0)));

    } catch (SQLException ex) {
      LOG.info("Unable to find tag key id \"{}\" in keytables.", key);
    }
    return keyString;
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
    String keyString = null;
    Integer keyInt = tag.getKey();
    String valueString = null;
    Integer valInt = tag.getValue();

    //check if Key and Value are in cache
    if (this.tagToString.containsKey(keyInt) && this.tagToString.get(keyInt).getValue().containsKey(valInt)) {
      return new ImmutablePair<>(this.tagToString.get(keyInt).getKey(), this.tagToString.get(keyInt).getValue().get(valInt));
    }

    //key or value is not in cache so let's go get them
    try (PreparedStatement valstmt = this.conn.prepareStatement("select k.TXT as KEYTXT,kv.TXT as VALUETXT from " + TableNames.E_KEYVALUE.toString() + " kv inner join " + TableNames.E_KEY.toString() + " k on k.ID = kv.KEYID WHERE k.ID = ? and kv.VALUEID = ?;")) {
      valstmt.setInt(1, keyInt);
      valstmt.setInt(2, valInt);
      ResultSet values = valstmt.executeQuery();
      values.next();
      keyString = values.getString("KEYTXT");
      valueString = values.getString("VALUETXT");

      //put it in caches
      Map<String, Integer> valResultString;
      Map<Integer, String> valResultInt;
      if (this.tagToInt.containsKey(keyString)) {
        valResultString = this.tagToInt.get(keyString).getValue();
        valResultInt = this.tagToString.get(keyInt).getValue();
      } else {
        valResultString = new ConcurrentHashMap<>(1);
        valResultInt = new ConcurrentHashMap<>(1);
      }
      valResultString.put(valueString, valInt);
      valResultInt.put(valInt, valueString);
      this.tagToInt.put(keyString, new ImmutablePair<>(keyInt, valResultString));
      this.tagToString.put(keyInt, new ImmutablePair<>(keyString, valResultInt));
    } catch (SQLException ex) {
      LOG.info("Unable to find tag id \"{}\"=\"{}\" in keytables.", tag.getKey(), tag.getValue());
    }

    return new ImmutablePair<>(keyString, valueString);

  }

  /**
   * Gets all values (ID and String) for a given key. To be used with care, as
   * it forcibly queries the DB each time it is called to be accurate.
   * Preferably call only once per key you are interested in.
   *
   * @param key The key to query all values for
   * @return a pair that holds the ID of the given key and all values with their
   * respective string and integer, or null if it cannot be found
   */
  @Deprecated
  public Pair<Integer, Map<String, Integer>> getAllValues(String key) {
    //search keyID
    Integer keyid = this.key2Int(key);
    HashMap<String, Integer> vals = null;
    if (keyid != null) {
      try (PreparedStatement valstmt = this.conn.prepareCall("select VALUEID,TXT from " + TableNames.E_KEYVALUE.toString() + " where KEYID=?;")) {
        valstmt.setString(1, keyid.toString());
        ResultSet values = valstmt.executeQuery();
        //get results
        vals = new HashMap<>(1);
        while (values.next()) {
          vals.put(values.getString("TXT"), values.getInt("VALUEID"));
        } //put results to cache
        this.tagToInt.put(key, new ImmutablePair<>(keyid, vals));
      } catch (SQLException ex) {
        LOG.info("Unable to find tag key \"{}\" in keytables.", key);
      }
    }

    return new ImmutablePair<>(keyid, vals);
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
    Integer roleInt = null;
    try (PreparedStatement rolestmt = conn.prepareStatement("select ID from " + TableNames.E_ROLE.toString() + " WHERE txt = ?;")) {
      rolestmt.setString(1, role);
      ResultSet roles = rolestmt.executeQuery();
      roles.next();
      roleInt = roles.getInt("ID");
      this.roleToInt.put(role, roleInt);
      this.roleToString.put(roleInt, role);

    } catch (SQLException ex) {
      LOG.info("Unable to find role \"{}\" in keytables.", role);
    }

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
    String roleString = null;
    try (PreparedStatement Rolestmt = conn.prepareStatement("select TXT from " + TableNames.E_ROLE.toString() + " WHERE ID = ?;")) {
      Rolestmt.setInt(1, role);
      ResultSet Roles = Rolestmt.executeQuery();
      Roles.next();
      roleString = Roles.getString("TXT");
      this.roleToInt.put(roleString, role);
      this.roleToString.put(role, roleString);

    } catch (SQLException ex) {
      LOG.info("Unable to find role id \"{}\" in keytables.", role);
    }

    return roleString;

  }

  /**
   * Get the OSHDB-UserID of a Username.
   *
   * @param name
   * @return
   */
  public Integer usertoID(String name) {
    if (this.userToInt.containsKey(name)) {
      return this.userToInt.get(name);
    }
    Integer uid = null;
    try (PreparedStatement userstmt = conn.prepareStatement("select ID from " + TableNames.E_USER.toString() + " WHERE NAME = ?;")) {
      userstmt.setString(1, name);
      ResultSet names = userstmt.executeQuery();
      names.next();
      uid = names.getInt("ID");
      this.userToInt.put(name, uid);
      this.userToString.put(uid, name);

    } catch (SQLException ex) {
      LOG.info("Unable to find user \"{}\" in keytables.", name);
    }

    return uid;

  }

  /**
   * Get the Username of a OSHDB-UserID.
   *
   * @param uid
   * @return
   */
  public String usertoStr(Integer uid) {
    if (this.userToString.containsKey(uid)) {
      return this.userToString.get(uid);
    }
    String name = null;
    try (PreparedStatement Userstmt = conn.prepareStatement("select NAME from " + TableNames.E_USER.toString() + " WHERE ID = ?;")) {
      Userstmt.setInt(1, uid);
      ResultSet Names = Userstmt.executeQuery();
      Names.next();
      name = Names.getString("NAME");
      this.userToInt.put(name, uid);
      this.userToString.put(uid, name);

    } catch (SQLException ex) {
      LOG.info("Unable to find user id \"{}\" in keytables.", uid);
    }

    return name;
  }

}
