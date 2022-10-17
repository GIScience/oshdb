package org.heigit.ohsome.oshdb.util.tagtranslator;

import static java.lang.String.format;
import static org.heigit.ohsome.oshdb.util.TableNames.E_KEY;
import static org.heigit.ohsome.oshdb.util.TableNames.E_KEYVALUE;
import static org.heigit.ohsome.oshdb.util.TableNames.E_ROLE;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTagOrRoleNotFoundException;
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
public class DefaultTagTranslator implements TagTranslator, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTagTranslator.class);
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
   * @throws SQLException
   */
  public DefaultTagTranslator(DataSource source) throws OSHDBKeytablesNotFoundException, SQLException {
    this.conn = source.getConnection();
    this.keyToInt = new ConcurrentHashMap<>(0);
    this.keyToString = new ConcurrentHashMap<>(0);
    this.tagToInt = new ConcurrentHashMap<>(0);
    this.tagToString = new ConcurrentHashMap<>(0);
    this.roleToInt = new ConcurrentHashMap<>(0);
    this.roleToString = new ConcurrentHashMap<>(0);

    // test connection for presence of actual "keytables" tables
    EnumSet<TableNames> keyTables =
        EnumSet.of(E_KEY, E_KEYVALUE, E_ROLE);
    for (TableNames table : keyTables) {
      try (Statement testTablePresentQuery = this.conn.createStatement()) {
        var selectSql = format("select 1 from %s limit 1", table);
        testTablePresentQuery.execute(selectSql);
      } catch (SQLException e) {
        throw new OSHDBKeytablesNotFoundException();
      }
    }

    // create prepared statements for querying tags from keytables
    try {
      keyIdQuery = conn.prepareStatement(format("select ID from %s where TXT = ?;", E_KEY));
      keyTxtQuery = conn.prepareStatement(format("select TXT from %s where ID = ?;", E_KEY));
      valueIdQuery = conn.prepareStatement(format("select k.ID as KEYID,kv.VALUEID as VALUEID"
          + " from %s kv"
          + " inner join %s k on k.ID = kv.KEYID"
          + " where k.TXT = ? and kv.TXT = ?;", E_KEYVALUE, E_KEY));
      valueTxtQuery = conn.prepareStatement(format("select k.TXT as KEYTXT,kv.TXT as VALUETXT"
          + " from %s kv"
          + " inner join %s k on k.ID = kv.KEYID"
          + " where k.ID = ? and kv.VALUEID = ?;", E_KEYVALUE, E_KEY));
      roleIdQuery = conn.prepareStatement(format("select ID from %s where TXT = ?;", E_ROLE));
      roleTxtQuery = conn.prepareStatement(format("select TXT from %s where ID = ?;", E_ROLE));
    } catch (SQLException e) {
      throw new OSHDBKeytablesNotFoundException();
    }
  }

  @Override
  public void close() throws SQLException {
    try {
      keyIdQuery.close();
      keyTxtQuery.close();
      valueIdQuery.close();
      valueTxtQuery.close();
      roleIdQuery.close();
      roleTxtQuery.close();
    } finally {
      conn.close();
    }
  }

  @Override
  public OSHDBTagKey getOSHDBTagKeyOf(String key) {
    return getOSHDBTagKeyOf(new OSMTagKey(key));
  }

  @Override
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
            LOG.debug("Unable to find tag key {} in keytables.", key);
            keyInt = new OSHDBTagKey(getFakeId(key.toString()));
          } else {
            keyInt = new OSHDBTagKey(keys.getInt("ID"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new OSHDBException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.keyToString.put(keyInt, key);
    this.keyToInt.put(key, keyInt);
    return keyInt;
  }

  @Override
  public OSHDBTag getOSHDBTagOf(String key, String value) {
    return this.getOSHDBTagOf(new OSMTag(key, value));
  }

  @Override
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
            tagInt = new OSHDBTag(
                this.getOSHDBTagKeyOf(tag.getKey()).toInt(), getFakeId(tag.getValue()));
          } else {
            tagInt = new OSHDBTag(values.getInt("KEYID"), values.getInt("VALUEID"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new OSHDBException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.tagToString.put(tagInt, tag);
    this.tagToInt.put(tag, tagInt);
    return tagInt;
  }

  @Override
  public OSMTag lookupTag(OSHDBTag tag) {
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
            throw new OSHDBTagOrRoleNotFoundException(format(
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
      throw new OSHDBException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    // put it in caches
    this.tagToInt.put(tagString, tag);
    this.tagToString.put(tag, tagString);
    return tagString;
  }

  @Override
  public OSHDBRole getOSHDBRoleOf(String role) {
    return this.getOSHDBRoleOf(new OSMRole(role));
  }

  @Override
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
            roleInt = OSHDBRole.of(getFakeId(role.toString()));
          } else {
            roleInt = OSHDBRole.of(roles.getInt("ID"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new OSHDBException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.roleToString.put(roleInt, role);
    this.roleToInt.put(role, roleInt);
    return roleInt;
  }

  @Override
  public OSMRole lookupRole(int role) {
    return this.lookupRole(OSHDBRole.of(role));
  }

  @Override
  public OSMRole lookupRole(OSHDBRole role) {
    if (this.roleToString.containsKey(role)) {
      return this.roleToString.get(role);
    }
    OSMRole roleString;
    try {
      synchronized (roleTxtQuery) {
        roleTxtQuery.setInt(1, role.getId());
        try (ResultSet roles = roleTxtQuery.executeQuery()) {
          if (!roles.next()) {
            throw new OSHDBTagOrRoleNotFoundException(format(
                "Unable to find role id %d in keytables.", role.getId()
            ));
          } else {
            roleString = new OSMRole(roles.getString("TXT"));
          }
        }
      }
    } catch (SQLException ex) {
      LOG.error(UNABLE_TO_ACCESS_KEYTABLES);
      throw new OSHDBException(UNABLE_TO_ACCESS_KEYTABLES);
    }
    this.roleToInt.put(roleString, role);
    this.roleToString.put(role, roleString);
    return roleString;
  }

  private int getFakeId(String s) {
    return -(s.hashCode() & 0x7fffffff);
  }
}
