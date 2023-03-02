package org.heigit.ohsome.oshdb.util.tagtranslator;

import static java.util.Collections.emptyMap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

public class MemoryTagTranslator implements TagTranslator {

  private final Map<String, Integer> strings = new HashMap<>();
  private final Cache<OSMTag, OSHDBTag> tags = Caffeine.newBuilder().build();
  private final Cache<OSMRole, OSHDBRole> roles = Caffeine.newBuilder().build();

  private final Cache<OSHDBTag, OSMTag> lookupTags = Caffeine.newBuilder().build();
  private final Cache<OSHDBRole, OSMRole> lookupRoles = Caffeine.newBuilder().build();

  @Override
  public Optional<OSHDBTagKey> getOSHDBTagKeyOf(OSMTagKey key) {
    return Optional.empty();
  }

  @Override
  public Map<OSMTag, OSHDBTag> getOSHDBTagOf(Collection<OSMTag> values, TRANSLATE_OPTION option) {
    return tags.getAll(values, set -> {
      if (option == TRANSLATE_OPTION.READONLY) {
        return emptyMap();
      }
      var map = Maps.<OSMTag, OSHDBTag>newHashMapWithExpectedSize(set.size());
      for(var osm : set) {
        var oshdb = new OSHDBTag(
          strings.computeIfAbsent(osm.getKey(), x -> strings.size()),
          strings.computeIfAbsent(osm.getValue(), x -> strings.size()));
        map.put(osm, oshdb);
      }
      return map;
    });
  }

  @Override
  public Map<OSMRole, OSHDBRole> getOSHDBRoleOf(Collection<OSMRole> values,
      TRANSLATE_OPTION option) {
    return roles.getAll(values, set -> {
      if (option == TRANSLATE_OPTION.READONLY) {
        return emptyMap();
      }
      var map = Maps.<OSMRole, OSHDBRole>newHashMapWithExpectedSize(set.size());
      for (var osm : set) {
        var oshdb = OSHDBRole.of(
            strings.computeIfAbsent(osm.toString(), x -> strings.size()));
        map.put(osm, oshdb);
      }
      return map;
    });
  }

  @Override
  public Map<OSHDBTag, OSMTag> lookupTag(Set<? extends OSHDBTag> tags) {
    return lookupTags.getAllPresent(tags);
  }

  @Override
  public OSMRole lookupRole(OSHDBRole role) {
    return lookupRoles.getIfPresent(role);
  }
}
