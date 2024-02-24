package org.heigit.ohsome.oshdb.util.tagtranslator;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.heigit.ohsome.oshdb.util.TableNames.E_ROLE;
import static org.heigit.ohsome.oshdb.util.tagtranslator.ClosableSqlArray.createArray;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;

@SuppressWarnings("java:S1192")
public class JdbcTagTranslator implements TagTranslator {

  private static final String OSM_OSHDB_KEY = """
      SELECT id, txt, values
      FROM tag_key k
      WHERE k.txt = any (?)""";

  private static final String MAX_KEY = "SELECT count(id) FROM tag_key";

  private static final String OSM_OSHDB_TAG = """
      SELECT keyid, valueid, kv.txt
      FROM tag_key k
      LEFT JOIN tag_value kv on k.id = kv.keyid
      WHERE k.txt = ? AND kv.txt = any (?)""";

  private static final String OSHDB_OSM_KEY = """
      SELECT txt, id
      FROM tag_key
      WHERE id = any(?)""";

  private static final String OSHDB_OSM_TAG = """
      SELECT txt, valueid
      FROM tag_value
      WHERE keyid = ? and valueid = any (?)""";

  private static final String ADD_OSHDB_KEY = "INSERT INTO tag_key values(?,?,?)";

  private static final String UPDATE_OSHDB_KEY = "UPDATE tag_key SET values= ? WHERE id = ?";

  private static final String ADD_OSHDB_TAG = "INSERT INTO tag_value values(?,?,?)";

  private static final String OSM_OSHDB_ROLE = """
      SELECT id, txt
      FROM role
      WHERE txt = any (?)""";

  private static final String OSHDB_OSM_ROLE = """
      SELECT txt, id
      FROM role
      WHERE id = any (?)""";

  private static final String ADD_OSHDB_ROLE = "INSERT INTO role values(?,?) ";


  private final DataSource source;
  private final Cache<Integer, String> cacheKeys;
  private final boolean readonly;

  /**
   * Attention: This tag translator relies on a pooled datasource for thread-safety.
   *
   * @param source   the (pooled) datasource
   * @param readonly marks this TagTranslator to not adding tags to the database.
   */
  public JdbcTagTranslator(DataSource source, boolean readonly) {
    this.source = source;
    cacheKeys = Caffeine.newBuilder().build();
    this.readonly = readonly;
  }

  /**
   * Attention: This tag translator relies on a pooled datasource for thread-safety.
   *
   * @param source the (pooled) datasource
   */
  public JdbcTagTranslator(DataSource source) {
    this(source, true);
  }

  @Override
  public Optional<OSHDBTagKey> getOSHDBTagKeyOf(OSMTagKey key) {
    try (var conn = source.getConnection();
        var sqlArray = createArray(conn, "text", Set.of(key.toString()));
        var pstmt = conn.prepareStatement(OSM_OSHDB_KEY)) {
      pstmt.setArray(1, sqlArray.get());
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
  public Map<OSMTag, OSHDBTag> getOSHDBTagOf(Collection<OSMTag> tags, TranslationOption option) {
    if (option != TranslationOption.READONLY && !readonly) {
      return getOrAddOSHDBTagsOf(tags);
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

  private synchronized Map<OSMTag, OSHDBTag> getOrAddOSHDBTagsOf(Collection<OSMTag> osmTags) {
    try {
      var keyTags = osmTags.stream()
          .collect(groupingBy(OSMTag::getKey, toMap(OSMTag::getValue, identity())));
      var keys = loadKeys(keyTags);

      var existing = keyTags.entrySet().parallelStream()
          .filter(entry -> keys.get(entry.getKey()).values() > 0)
          .flatMap(entry -> loadTags(entry.getKey(), entry.getValue()).entrySet().stream())
          .collect(toMap(Entry::getKey, Entry::getValue));

      var missingKeyTags = osmTags.stream().filter(not(existing::containsKey))
          .collect(groupingBy(OSMTag::getKey, toMap(OSMTag::getValue, identity())));
      var newTags = missingKeyTags.entrySet().parallelStream()
          .flatMap(entry -> addTags(keys.get(entry.getKey()), entry.getValue()).entrySet().stream())
          .collect(toMap(Entry::getKey, Entry::getValue));
      existing.putAll(newTags);
      return existing;
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }

  private Map<String, KeyValues> loadKeys(Map<String, Map<String, OSMTag>> keyTags)
      throws SQLException {
    try (var conn = source.getConnection()) {
      var keys = Maps.<String, KeyValues>newHashMapWithExpectedSize(keyTags.size());
      try (var sqlArray = createArray(conn, "text", keyTags.keySet());
          var pstmt = conn.prepareStatement(OSM_OSHDB_KEY)) {
        pstmt.setArray(1, sqlArray.get());
        try (var rst = pstmt.executeQuery()) {
          while (rst.next()) {
            var keyValues = new KeyValues(rst.getInt(1), rst.getString(2), rst.getInt(3));
            keys.put(keyValues.txt(), keyValues);
          }
        }
      }
      var newKeys = keyTags.keySet().stream().filter(not(keys::containsKey)).collect(toSet());
      if (!newKeys.isEmpty()) {
        var nextKeyId = nextKeyId(conn);
        for (var newKey : newKeys) {
          keys.put(newKey, new KeyValues(nextKeyId++, newKey, 0));
        }
      }
      return keys;
    }
  }

  private int nextKeyId(Connection conn) throws SQLException {
    try (var stmt = conn.createStatement();
        var rst = stmt.executeQuery(MAX_KEY)) {
      if (!rst.next()) {
        throw new NoSuchElementException();
      }
      return rst.getInt(1);
    }
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

  private Map<OSMTag, OSHDBTag> addTags(KeyValues keyValues, Map<String, OSMTag> tags) {
    var map = Maps.<OSMTag, OSHDBTag>newHashMapWithExpectedSize(tags.size());
    try (var conn = source.getConnection();
        var addKey = keyValues.values() == 0 ? conn.prepareStatement(ADD_OSHDB_KEY)
            : conn.prepareStatement(UPDATE_OSHDB_KEY);
        var addTag = conn.prepareStatement(ADD_OSHDB_TAG)) {

      var keyId = keyValues.id();
      var keyTxt = keyValues.txt();
      var nextValueId = keyValues.values();

      var batchSize = 0;
      for (var entry : tags.entrySet()) {
        var txt = entry.getKey();
        var osm = entry.getValue();
        var oshdb = new OSHDBTag(keyId, nextValueId++);
        addTag.setInt(1, oshdb.getKey());
        addTag.setInt(2, oshdb.getValue());
        addTag.setString(3, txt);
        addTag.addBatch();
        batchSize++;
        if (batchSize >= 1000) {
          addTag.executeBatch();
          batchSize = 0;
        }
        map.put(osm, oshdb);
      }
      addTag.executeBatch();
      if (keyValues.values() == 0) {
        addKey.setInt(1, keyId);
        addKey.setString(2, keyTxt);
        addKey.setInt(3, nextValueId);
      } else {
        addKey.setInt(1, nextValueId);
        addKey.setInt(2, keyId);
      }
      addKey.executeUpdate();
      return map;
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
  }

  @Override
  public Map<OSMRole, OSHDBRole> getOSHDBRoleOf(Collection<OSMRole> roles,
      TranslationOption option) {
    if (option != TranslationOption.READONLY && !readonly) {
      return getOrAddOSHDBRoleOf(roles);
    }
    return loadRoles(roles);
  }

  private synchronized Map<OSMRole, OSHDBRole> getOrAddOSHDBRoleOf(Collection<OSMRole> roles) {
    var existing = loadRoles(roles);
    var missing = roles.stream().filter(not(existing::containsKey)).collect(toSet());
    try (var conn = source.getConnection();
        var pstmt = conn.prepareStatement(ADD_OSHDB_ROLE)) {
      var nextRoleId = nextRoleId(conn);
      for (var osm : missing) {
        var oshdb = OSHDBRole.of(nextRoleId++);
        pstmt.setInt(1, oshdb.getId());
        pstmt.setString(2, osm.toString());
        pstmt.addBatch();
        existing.put(osm, oshdb);
      }
      pstmt.executeBatch();
    } catch (SQLException e) {
      throw new OSHDBException(e);
    }
    return existing;
  }

  private int nextRoleId(Connection conn) throws SQLException {
    try (var stmt = conn.createStatement();
        var rst = stmt.executeQuery(format("select count(*) from %s", E_ROLE))) {
      if (!rst.next()) {
        throw new NoSuchElementException();
      }
      return rst.getInt(1);
    }
  }

  private Map<OSMRole, OSHDBRole> loadRoles(Collection<OSMRole> roles) {
    try (var conn = source.getConnection();
        var sqlArray = createArray(conn, "text", roles.stream().map(OSMRole::toString).toList());
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
  public Map<OSHDBTagKey, OSMTagKey> lookupKey(Set<? extends OSHDBTagKey> oshdbTagKeys) {
    var keys = oshdbTagKeys.stream().map(OSHDBTagKey::toInt).collect(toSet());
    return cacheKeys.getAll(keys, this::lookupKeys).entrySet()
        .stream()
        .collect(toMap(entry -> new OSHDBTagKey(entry.getKey()),
            entry -> new OSMTagKey(entry.getValue())));
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

  private Map<OSHDBTag, OSMTag> lookupTags(int keyId, String keyTxt,
      Map<Integer, OSHDBTag> values) {
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
            createArray(conn, "int", roles.stream().map(OSHDBRole::getId).toList());
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

  private record KeyValues(int id, String txt, int values) {

    @Override
      public String toString() {
        return "KeyValues{" +
            "id=" + id +
            ", txt='" + txt + '\'' +
            ", values=" + values +
            '}';
      }
    }
}
