package org.heigit.ohsome.oshdb.util.tagtranslator;

import static java.lang.String.format;
import static java.util.function.Predicate.not;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;

public class AddingJdbcTagTranslator extends JdbcTagTranslator implements AddingTagTranslator {

  private static final String LOCK_TABLE = "locks";

  private static final String LOCK = "updater";

  private static final String OSM_OSHDB_KEYS = format("SELECT id, txt, values"
      + " FROM %s WHERE txt = any(?)", TableNames.E_KEY);

  private static final String INSERT_OSM_OSHDB_KEYS = format("INSERT INTO %s"
      + " (id, txt, values) VALUES (?, ?, ?)", TableNames.E_KEY);

  private static final String UPDATE_OSM_OSHDB_KEYS = format("UPDATE %s"
      + " SET values = ? WHERE id = ?", TableNames.E_KEY);

  private static final String INSERT_OSM_OSHDB_TAG = format("INSERT INTO %s"
      + " (keyid, valueid, txt) VALUES (?, ?, ?)", TableNames.E_KEYVALUE);

  private static final String CREATE_LOG_TABLE = format("CREATE TABLE IF NOT EXISTS %s"
      + " (lock VARCHAR PRIMARY KEY, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP", LOCK_TABLE);

  private static final String TRY_TO_LOCK = format("INSERT INTO %s (lock) VALUES (?)", LOCK_TABLE);

  private static final String RELEASE_LOCK = format("DELETE FROM %s WHERE lock = ?", LOCK_TABLE);


  public static final int INSERT_MAX_BATCH_COUNT = 10_000;


  private static class KeyIdValueSequence {

    private final int keyId;
    private int nextValueId;
    private final boolean isNew;

    public KeyIdValueSequence(int keyId, int nextValueId) {
      this.keyId = keyId;
      this.nextValueId = nextValueId;
      isNew = nextValueId == 0;
    }

    public int getKeyId() {
      return keyId;
    }

    public int getNextValueId() {
      return nextValueId++;
    }
  }

  private final Cache<String, KeyIdValueSequence> cacheOSMKeys;
  private final AtomicInteger keySequence;

  /**
   * Attention: This tag translator relies on a pooled datasource for thread-safety.
   *
   * @param source the (pooled) datasource
   */
  public AddingJdbcTagTranslator(DataSource source) {
    super(source);
    createLockTable();
    cacheOSMKeys = Caffeine.newBuilder().build();
    keySequence = new AtomicInteger(getMaxKeyId());
  }

  private void createLockTable(){
    try (var conn = source.getConnection();
         var stmt = conn.createStatement()) {
      stmt.executeUpdate(CREATE_LOG_TABLE);
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }

  public void tryToLock() {
    try (var conn = source.getConnection();
         var pstmt = conn.prepareStatement(TRY_TO_LOCK)) {
      pstmt.setString(1, LOCK);
      if (pstmt.executeUpdate() != 1) {
        throw new OSHDBException("could not insert lock");
      }
    } catch (Exception e) {
      throw new OSHDBException("could not acquire lock \"" + LOCK +"\"", e);
    }
  }

  public void releaseLock() {
    try (var conn = source.getConnection();
        var pstmt = conn.prepareStatement(RELEASE_LOCK)) {
      pstmt.setString(1, LOCK);
      if (pstmt.executeUpdate() != 1) {
        throw new OSHDBException("could not release lock");
      }
    } catch (Exception e) {
      throw new OSHDBException("could not release lock \"" + LOCK +"\"", e);
    }
  }

  private int getMaxKeyId() {
    try (var conn = source.getConnection();
        var stmt = conn.createStatement();
        var rst = stmt.executeQuery("SELECT max(id) from " + TableNames.E_KEY)) {
      if (!rst.next()) {
        throw new OSHDBException("couldn't retrieve max key id");
      }
      return rst.getInt(1);
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public synchronized Map<OSMTag, OSHDBTag> getOrAddOSHDBTagOf(Collection<OSMTag> tags) {
      var present = getOSHDBTagOf(tags);
      var missing = Maps.<String, Map<String, OSMTag>>newHashMapWithExpectedSize(
          tags.size() - present.size());
      tags.stream()
          .filter(not(present::containsKey))
          .forEach(tag -> missing.computeIfAbsent(tag.getKey(), x -> new HashMap<>())
              .put(tag.getValue(), tag));
      try (var conn = source.getConnection()) {
        var keys = cacheOSMKeys.getAll(missing.keySet(),
            missingKeys -> loadKeys(conn, missingKeys));
        var result = addTags(conn, missing, keys);
        updateKeys(conn, keys);
        return result;
      } catch (Exception e) {
        throw new OSHDBException(e);
      }
  }

  private Map<String, KeyIdValueSequence> loadKeys(Connection conn, Set<? extends String> keys) {
    try (var sqlArray = ClosableSqlArray.createArray(conn, "text", keys);
        var pstmt = conn.prepareStatement(OSM_OSHDB_KEYS)){
      pstmt.setArray(1, sqlArray.get());
      try (var rst = pstmt.executeQuery()) {
        var map = Maps.<String, KeyIdValueSequence>newHashMapWithExpectedSize(keys.size());
        while (rst.next()) {
          var id = rst.getInt(1);
          var txt = rst.getString(2);
          var values = rst.getInt(3);
          map.put(txt, new KeyIdValueSequence(id, values));
        }
        keys.stream()
            .filter(not(map::containsKey))
            .forEach(newKey -> map.put(newKey, new KeyIdValueSequence(keySequence.getAndIncrement(), 0)));
        return map;
      }
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }

  private Map<OSMTag, OSHDBTag> addTags(Connection conn, Map<String, Map<String, OSMTag>> tags,
      Map<String, KeyIdValueSequence> keys) {
    try (var pstmt = conn.prepareStatement(INSERT_OSM_OSHDB_TAG)) {
      var map = Maps.<OSMTag, OSHDBTag>newHashMapWithExpectedSize(tags.size());
      var batch = INSERT_MAX_BATCH_COUNT;
      for (var entry : tags.entrySet()) {
        var key = keys.get(entry.getKey());
        for (var tag : entry.getValue().entrySet()) {
          var oshdb = new OSHDBTag(key.getKeyId(), key.getNextValueId());
          map.put(tag.getValue(), oshdb);
          pstmt.setInt(1, oshdb.getKey());
          pstmt.setInt(2, oshdb.getValue());
          pstmt.setString(3, tag.getKey());
          pstmt.addBatch();
          if (--batch == 0) {
            pstmt.executeBatch();
            batch = INSERT_MAX_BATCH_COUNT;
          }
        }
      }
      pstmt.executeBatch();
      return map;
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }

  private void updateKeys(Connection conn, Map<String, KeyIdValueSequence> keys) {
    try ( var insert = conn.prepareStatement(INSERT_OSM_OSHDB_KEYS);
          var update = conn.prepareStatement(UPDATE_OSM_OSHDB_KEYS)) {
      for (var entry : keys.entrySet()) {
        var keyTxt = entry.getKey();
        var keyIdValueSequence = entry.getValue();
        var keyId = keyIdValueSequence.keyId;
        var values = keyIdValueSequence.nextValueId;
        if (keyIdValueSequence.isNew) {
          insert.setInt(1, keyId);
          insert.setString(2, keyTxt);
          insert.setInt(3, values);
          insert.addBatch();
        } else {
          update.setInt(1, values);
          update.setInt(2, keyId);
          update.addBatch();
        }
      }
      insert.executeBatch();
      update.executeBatch();
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }
}