package org.heigit.ohsome.oshdb.util.tagtranslator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

public class CachedTagTranslator implements TagTranslator {

  private TagTranslator source;

  private Cache<OSHDBTag, OSMTag> lookupOSHDBTag;
  private Cache<OSHDBRole, OSMRole> lookupOSHDBRole;

  public CachedTagTranslator(TagTranslator source, long maxBytesValues, int maxNumRoles) {
    this.source = source;
    this.lookupOSHDBTag = Caffeine.newBuilder()
        .<OSHDBTag, OSMTag>weigher((oshdb, osm) -> osm.getValue().length() * 2)
        .maximumWeight(maxBytesValues)
        .build();
    this.lookupOSHDBRole = Caffeine.newBuilder()
        .maximumSize(maxNumRoles)
        .build();
  }

  public Cache<OSHDBTag, OSMTag> getLookupOSHDBTag() {
    return lookupOSHDBTag;
  }

  public Cache<OSHDBRole, OSMRole> getLookupOSHDBRole() {
    return lookupOSHDBRole;
  }

  @Override
  public Optional<OSHDBTagKey> getOSHDBTagKeyOf(OSMTagKey key) {
    return source.getOSHDBTagKeyOf(key);
  }

  @Override
  public Optional<OSHDBTag> getOSHDBTagOf(OSMTag osm) {
    var oshdb = source.getOSHDBTagOf(osm);
    oshdb.ifPresent(tag -> lookupOSHDBTag.put(tag, osm));
    return oshdb;
  }

  @Override
  public Map<OSMTag, OSHDBTag> getOSHDBTagOf(Set<? extends OSMTag> tags) {
    var oshdb = source.getOSHDBTagOf(tags);
    oshdb.entrySet().forEach(entry -> lookupOSHDBTag.put(entry.getValue(), entry.getKey()));
    return oshdb;
  }

  @Override
  public Optional<OSHDBRole> getOSHDBRoleOf(OSMRole role) {
    return source.getOSHDBRoleOf(role);
  }

  @Override
  public Map<OSMRole, OSHDBRole> getOSHDBRoleOf(Set<? extends OSMRole> roles) {
    return source.getOSHDBRoleOf(roles);
  }

  @Override
  public OSMTag lookupTag(OSHDBTag tag) {
    return lookupOSHDBTag.get(tag, source::lookupTag);
  }

  @Override
  public Map<OSHDBTag, OSMTag> lookupTag(Set<? extends OSHDBTag> tags) {
    return lookupOSHDBTag.getAll(tags, source::lookupTag);
  }

  @Override
  public OSMRole lookupRole(OSHDBRole role) {
    return lookupOSHDBRole.get(role, source::lookupRole);
  }
}
