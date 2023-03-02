package org.heigit.ohsome.oshdb.util.tagtranslator;

import static java.util.stream.Collectors.toList;
import static org.heigit.ohsome.oshdb.util.tagtranslator.ClosableSqlArray.createArray;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;

@SuppressWarnings("java:S1192")
public class JdbcTagTranslator implements TagTranslator {

  private static final String OSM_OSHDB_KEY = String.format("SELECT id, txt"
      + " from %s k"
      + " where k.txt = ?", TableNames.E_KEY);

  private static final String OSM_OSHDB_TAG = String.format("SELECT keyid, valueid, kv.txt"
      + " from %s k"
      + " left join %s kv on k.id = kv.keyid"
      + " where k.txt = ? and kv.txt = any (?)", TableNames.E_KEY, TableNames.E_KEYVALUE);

  private static final String OSHDB_OSM_KEY = String.format("SELECT txt, id"
      + " from %s"
      + " where id = any(?)", TableNames.E_KEY);

  private static final String OSHDB_OSM_TAG = String.format("SELECT txt, valueid"
      + " from %s"
      + " where keyid = ? and valueid = any (?)", TableNames.E_KEYVALUE);

  private static final String OSM_OSHDB_ROLE = String.format("SELECT id, txt"
      + " from %s"
      + " where txt = any (?)", TableNames.E_ROLE);

  private static final String OSHDB_OSM_ROLE = String.format("SELECT txt, id"
      + " from %s"
      + " where id = any (?)", TableNames.E_ROLE);

  private final DataSource source;
  private final Cache<Integer, String> cacheKeys;

  /**
   * Attention:
   * This tag translator relies on a pooled datasource for thread-safety.
   *
   * @param source the (pooled) datasource
   */
  public JdbcTagTranslator(DataSource source) {
    this.source = source;
    cacheKeys = Caffeine.newBuilder()
            .build();
  }

  @Override
  public Optional<OSHDBTagKey> getOSHDBTagKeyOf(OSMTagKey key) {
    try (var conn = source.getConnection();
         var pstmt = conn.prepareStatement(OSM_OSHDB_KEY)) {
      pstmt.setString(1, key.toString());
      try (var rst = pstmt.executeQuery()) {
        if (rst.next()) {
          return Optional.of(new OSHDBTagKey(rst.getInt(1)));
        }
        return Optional.empty();
      }
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public Map<OSMTag, OSHDBTag> getOSHDBTagOf(Collection<OSMTag> tags, TRANSLATE_OPTION option) {
    if (option != TRANSLATE_OPTION.READONLY) {
      throw new UnsupportedOperationException("mutating jdbc translator is not supported yet");
    }
    var keyTags = Maps.<String, Map<String, OSMTag>>newHashMapWithExpectedSize(tags.size());
    tags.forEach(tag -> keyTags.computeIfAbsent(tag.getKey(), x -> new HashMap<>())
        .put(tag.getValue(), tag));
    var result = Maps.<OSMTag, OSHDBTag>newConcurrentMap();
    keyTags.entrySet().parallelStream()
      .map(entry -> loadTags(entry.getKey(), entry.getValue()))
      .forEach(result::putAll);
    return result;
  }



  private Map<OSMTag, OSHDBTag> loadTags(String key, Map<String, OSMTag> values) {
    try (var conn = source.getConnection();
         var sqlArray = createArray(conn, "text", values.keySet());
         var pstmt = conn.prepareStatement(OSM_OSHDB_TAG)) {
      pstmt.setString(1, key);
      pstmt.setArray(2, sqlArray.get());
      try (var rst = pstmt.executeQuery()) {
        var map = Maps.<OSMTag, OSHDBTag>newHashMapWithExpectedSize(values.size());
        while (rst.next()) {
          var keyId = rst.getInt(1);
          var valId = rst.getInt(2);
          var value = rst.getString(3);
          map.put(values.get(value), new OSHDBTag(keyId, valId));
        }
        return map;
      }
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public Map<OSMRole, OSHDBRole> getOSHDBRoleOf(Collection<OSMRole> roles, TRANSLATE_OPTION option) {
    if (option != TRANSLATE_OPTION.READONLY) {
      throw new UnsupportedOperationException("mutating jdbc translator is not supported yet");
    }
    return loadRoles(roles);
  }

  private Map<OSMRole, OSHDBRole> loadRoles(Collection<OSMRole> roles) {
    try (var conn = source.getConnection();
        var sqlArray =
            createArray(conn, "text", roles.stream().map(OSMRole::toString).collect(toList()));
        var pstmt = conn.prepareStatement(OSM_OSHDB_ROLE)) {
      pstmt.setArray(1, sqlArray.get());
      try (var rst = pstmt.executeQuery()) {
        var map = Maps.<OSMRole, OSHDBRole>newHashMapWithExpectedSize(roles.size());
        while (rst.next()) {
          var id = rst.getInt(1);
          var txt = rst.getString(2);
          map.put(new OSMRole(txt), OSHDBRole.of(id));
        }
        return map;
      }
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public OSMTag lookupTag(OSHDBTag tag) {
    var keyTxt = cacheKeys.getAll(Set.of(tag.getKey()), this::lookupKeys).get(tag.getKey());
    return lookupTags(tag.getKey(), keyTxt, Map.of(tag.getValue(), tag)).get(tag);
  }



  @Override
  public Map<OSHDBTag, OSMTag> lookupTag(Set<? extends OSHDBTag> tags) {
    var keyTags = Maps.<Integer, Map<Integer, OSHDBTag>>newHashMapWithExpectedSize(tags.size());
    tags.forEach(tag -> keyTags.computeIfAbsent(tag.getKey(), x -> new HashMap<>())
        .put(tag.getValue(), tag));
    var keys = cacheKeys.getAll(keyTags.keySet(), this::lookupKeys);
    var result = Maps.<OSHDBTag, OSMTag>newConcurrentMap();
    keyTags.entrySet().parallelStream()
      .map(entry -> lookupTags(entry.getKey(), keys.get(entry.getKey()), entry.getValue()))
      .forEach(result::putAll);
    return result;
  }

  private Map<Integer, String> lookupKeys(Set<? extends Integer> osm) {
    try (var conn = source.getConnection();
         var sqlArray = createArray(conn, "int", osm);
         var pstmt = conn.prepareStatement(OSHDB_OSM_KEY)) {
      pstmt.setArray(1, sqlArray.get());
      try (var rst = pstmt.executeQuery()) {
        var map = Maps.<Integer, String>newHashMapWithExpectedSize(osm.size());
        while (rst.next()) {
          var keyTxt = rst.getString(1);
          var keyId = rst.getInt(2);
          map.put(keyId, keyTxt);
        }
        return map;
      }
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }


  private Map<OSHDBTag, OSMTag> lookupTags(int keyId, String keyTxt, Map<Integer, OSHDBTag> values) {
    try (var conn = source.getConnection();
        var sqlArray = createArray(conn, "int", values.keySet());
        var pstmt = conn.prepareStatement(OSHDB_OSM_TAG)) {
      pstmt.setInt(1, keyId);
      pstmt.setArray(2, sqlArray.get());
      try (var rst = pstmt.executeQuery()) {
        var map = Maps.<OSHDBTag, OSMTag>newHashMapWithExpectedSize(values.size());
        while (rst.next()) {
          var valTxt = rst.getString(1);
          var valId = rst.getInt(2);
          map.put(values.get(valId), new OSMTag(keyTxt, valTxt));
        }
        return map;
      }
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public OSMRole lookupRole(OSHDBRole role) {
    return lookupRoles(Set.of(role)).get(role);
  }

  private Map<OSHDBRole, OSMRole> lookupRoles(Set<? extends OSHDBRole> roles) {
    try (var conn = source.getConnection();
        var sqlArray =
            createArray(conn, "int", roles.stream().map(OSHDBRole::getId).collect(toList()));
        var pstmt = conn.prepareStatement(OSHDB_OSM_ROLE)) {
      pstmt.setArray(1, sqlArray.get());
      try (var rst = pstmt.executeQuery()) {
        var map = Maps.<OSHDBRole, OSMRole>newHashMapWithExpectedSize(roles.size());
        while (rst.next()) {
          var txt = rst.getString(1);
          var id = rst.getInt(2);
          map.put(OSHDBRole.of(id), new OSMRole(txt));
        }
        return map;
      }
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }
}
