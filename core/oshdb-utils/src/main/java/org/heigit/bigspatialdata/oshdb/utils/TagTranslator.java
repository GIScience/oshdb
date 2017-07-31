package org.heigit.bigspatialdata.oshdb.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Easily translate your String-Keys or Values to OSHDb integer and vice versa.
 *
 */
public class TagTranslator {

  private static final Logger LOG = Logger.getLogger(TagTranslator.class.getName());
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
   */
  public TagTranslator(Connection conn) {
    this.conn = conn;
    this.tagToInt = new HashMap<>(0);
    this.tagToString = new HashMap<>(0);
    this.roleToInt = new HashMap<>(0);
    this.roleToString = new HashMap<>(0);
    this.userToInt = new HashMap<>(0);
    this.userToString = new HashMap<>(0);
  }

  /**
   * Get the ID for your Tag.
   *
   * @param Tag A Key-Value-Pair
   * @return The correspondent Key-Value-Pair as Integer
   */
  public Pair<Integer, Integer> Tag2Int(Pair<String, String> Tag) {
    String KeyString = Tag.getKey();
    Integer KeyInt = null;
    String ValueString = Tag.getValue();
    Integer ValInt = null;

    //check if Key and Value are in cache
    if (this.tagToInt.containsKey(KeyString) && this.tagToInt.get(KeyString).getValue().containsKey(ValueString)) {
      return new ImmutablePair<>(this.tagToInt.get(KeyString).getKey(), this.tagToInt.get(KeyString).getValue().get(ValueString));
    }

    //key or value is not in cache so let's go get them
    try (PreparedStatement valstmt = this.conn.prepareStatement("select k.ID as KEYID,kv.VALUEID as VALUEID from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID WHERE k.TXT = ? and kv.TXT = ?;")) {
      valstmt.setString(1, KeyString);
      valstmt.setString(2, ValueString);
      ResultSet values = valstmt.executeQuery();
      values.next();
      KeyInt = values.getInt("KEYID");
      ValInt = values.getInt("VALUEID");

      //put it in caches
      Map<String, Integer> valresultstr;
      Map<Integer, String> valresultint;
      if (this.tagToInt.containsKey(KeyString)) {
        valresultstr = this.tagToInt.get(KeyString).getValue();
        valresultint = this.tagToString.get(KeyInt).getValue();
      } else {
        valresultstr = new HashMap<>(1);
        valresultint = new HashMap<>(1);
      }
      valresultstr.put(ValueString, ValInt);
      valresultint.put(ValInt, ValueString);
      this.tagToInt.put(KeyString, new ImmutablePair<>(KeyInt, valresultstr));
      this.tagToString.put(KeyInt, new ImmutablePair<>(KeyString, valresultint));
    } catch (SQLException ex) {
      LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
    }

    return new ImmutablePair<>(KeyInt, ValInt);

  }

  /**
   * Get Integer of a single Key.
   *
   * @param Key
   * @return
   */
  public Integer Key2Int(String Key) {
    if (this.tagToInt.containsKey(Key)) {
      return this.tagToInt.get(Key).getKey();
    }
    Integer key = null;
    try (PreparedStatement keystmt = this.conn.prepareStatement("select ID from KEY where KEY.TXT = ?;")) {
      keystmt.setString(1, Key);
      ResultSet keys = keystmt.executeQuery();
      keys.next();
      key = keys.getInt("ID");
      this.tagToInt.put(Key, new ImmutablePair<>(key, new HashMap<>(0)));
      this.tagToString.put(key, new ImmutablePair<>(Key, new HashMap<>(0)));

    } catch (SQLException ex) {
      LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
    }
    return key;
  }

  /**
   * Get the String for your ID.
   *
   * @param Tag
   * @return
   */
  public Pair<String, String> Tag2String(Pair<Integer, Integer> Tag) {
    String KeyString = null;
    Integer KeyInt = Tag.getKey();
    String ValueString = null;
    Integer ValInt = Tag.getValue();

    //check if Key and Value are in cache
    if (this.tagToString.containsKey(KeyInt) && this.tagToString.get(KeyInt).getValue().containsKey(ValInt)) {
      return new ImmutablePair<>(this.tagToString.get(KeyInt).getKey(), this.tagToString.get(KeyInt).getValue().get(ValInt));
    }

    //key or value is not in cache so let's go get them
    try (PreparedStatement valstmt = this.conn.prepareStatement("select k.TXT as KEYTXT,kv.TXT as VALUETXT from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID WHERE k.ID = ? and kv.VALUEID = ?;")) {
      valstmt.setInt(1, KeyInt);
      valstmt.setInt(2, ValInt);
      ResultSet values = valstmt.executeQuery();
      values.next();
      KeyString = values.getString("KEYTXT");
      ValueString = values.getString("VALUETXT");

      //put it in caches
      Map<String, Integer> valresultstr;
      Map<Integer, String> valresultint;
      if (this.tagToInt.containsKey(KeyString)) {
        valresultstr = this.tagToInt.get(KeyString).getValue();
        valresultint = this.tagToString.get(KeyInt).getValue();
      } else {
        valresultstr = new HashMap<>(1);
        valresultint = new HashMap<>(1);
      }
      valresultstr.put(ValueString, ValInt);
      valresultint.put(ValInt, ValueString);
      this.tagToInt.put(KeyString, new ImmutablePair<>(KeyInt, valresultstr));
      this.tagToString.put(KeyInt, new ImmutablePair<>(KeyString, valresultint));
    } catch (SQLException ex) {
      LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
    }

    return new ImmutablePair<>(KeyString, ValueString);

  }

  /**
   * Gets all values (ID and String) for a given Key. To be used with care, as
   * it forcibly queries the DB each time it is called to be accurate.
   * Preferably call only once per Key you are interested in.
   *
   * @param Key The key to query all values for
   * @return a pair that holds the ID of the given key and all values with their
   * respective string and integer
   */
  public Pair<Integer, Map<String, Integer>> getAllValues(String Key) {
    //search keyID
    Integer keyid = this.Key2Int(Key);
    HashMap<String, Integer> vals = null;
    if (keyid != null) {
      try (PreparedStatement valstmt = this.conn.prepareCall("select VALUEID,TXT from KEYVALUE where KEYID=?;")) {
        valstmt.setString(1, keyid.toString());
        ResultSet values = valstmt.executeQuery();
        //get results
        vals = new HashMap<>(1);
        while (values.next()) {
          vals.put(values.getString("TXT"), values.getInt("VALUEID"));
        } //put results to cache
        this.tagToInt.put(Key, new ImmutablePair<>(keyid, vals));
      } catch (SQLException ex) {
        LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
      }
    }

    return new ImmutablePair<>(keyid, vals);

  }

  /**
   * Get the ID for your Role.
   *
   * @param Role
   * @return
   */
  public Integer Role2Int(String Role) {
    if (this.roleToInt.containsKey(Role)) {
      return this.roleToInt.get(Role);
    }
    Integer role = null;
    try (PreparedStatement rolestmt = conn.prepareStatement("select ID from ROLE WHERE txt = ?;")) {
      rolestmt.setString(1, Role);
      ResultSet roles = rolestmt.executeQuery();
      roles.next();
      role = roles.getInt("ID");
      this.roleToInt.put(Role, role);
      this.roleToString.put(role, Role);

    } catch (SQLException ex) {
      LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
    }

    return role;

  }

  /**
   * Get the String for your Role.
   *
   * @param role
   * @return
   */
  public String Role2String(Integer role) {
    if (this.roleToString.containsKey(role)) {
      return this.roleToString.get(role);
    }
    String Role = null;
    try (PreparedStatement Rolestmt = conn.prepareStatement("select TXT from ROLE WHERE ID = ?;")) {
      Rolestmt.setInt(1, role);
      ResultSet Roles = Rolestmt.executeQuery();
      Roles.next();
      Role = Roles.getString("TXT");
      this.roleToInt.put(Role, role);
      this.roleToString.put(role, Role);

    } catch (SQLException ex) {
      LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
    }

    return Role;

  }

  /**
   * Get the OSHDB-UserID of a Username.
   *
   * @param Name
   * @return
   */
  public Integer UsertoID(String Name) {
    if (this.userToInt.containsKey(Name)) {
      return this.userToInt.get(Name);
    }
    Integer OSHDBuserID = null;
    try (PreparedStatement userstmt = conn.prepareStatement("select ID from USER WHERE NAME = ?;")) {
      userstmt.setString(1, Name);
      ResultSet names = userstmt.executeQuery();
      names.next();
      OSHDBuserID = names.getInt("ID");
      this.userToInt.put(Name, OSHDBuserID);
      this.userToString.put(OSHDBuserID, Name);

    } catch (SQLException ex) {
      LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
    }

    return OSHDBuserID;

  }

  /**
   * Get the Username of a OSHDB-UserID.
   *
   * @param OSHDBuserID
   * @return
   */
  public String UsertoStr(Integer OSHDBuserID) {
    if (this.userToString.containsKey(OSHDBuserID)) {
      return this.userToString.get(OSHDBuserID);
    }
    String Name = null;
    try (PreparedStatement Userstmt = conn.prepareStatement("select NAME from USER WHERE ID = ?;")) {
      Userstmt.setInt(1, OSHDBuserID);
      ResultSet Names = Userstmt.executeQuery();
      Names.next();
      Name = Names.getString("NAME");
      this.roleToInt.put(Name, OSHDBuserID);
      this.roleToString.put(OSHDBuserID, Name);

    } catch (SQLException ex) {
      LOG.log(Level.WARNING, "Either the connection faild, or there was no result", ex);
    }

    return Name;
  }

}
