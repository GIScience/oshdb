package org.heigit.ohsome.oshdb.mock;

import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.TagTranslator;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMRole;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;

public class MockTranslator implements TagTranslator {
  private final Map<OSMTag, OSHDBTag> translate = new ConcurrentHashMap<>();
  private final Map<OSHDBTag, OSMTag> lookup = new ConcurrentHashMap<>();
  private final Map<OSMRole, OSHDBRole> translateRole = new ConcurrentHashMap<>();
  private final Map<OSHDBRole, OSMRole> lookupRole = new ConcurrentHashMap<>();
  private final Map<String, Integer> stringIds = new ConcurrentHashMap<>();
  private final AtomicInteger nextStringId = new AtomicInteger();

  @Override
  public Map<OSMTag, OSHDBTag> getOSHDBTagOf(Collection<? extends OSMTag> tags) {
    var map = Maps.<OSMTag, OSHDBTag>newHashMapWithExpectedSize(tags.size());
    tags.forEach(tag -> map.put(tag, translate(tag)));
    return map;
  }

  @Override
  public Map<OSMRole, OSHDBRole> getOSHDBRoleOf(Collection<? extends OSMRole> roles) {
    var map = Maps.<OSMRole, OSHDBRole>newHashMapWithExpectedSize(roles.size());
    roles.forEach(role -> map.put(role, translate(role)));
    return map;
  }

  @Override
  public Map<OSHDBTag, OSMTag> lookupTag(Set<? extends OSHDBTag> tags) {
    var map = Maps.<OSHDBTag, OSMTag>newHashMapWithExpectedSize(tags.size());
    tags.forEach(tag -> map.put(tag, lookup.get(tag)));
    return map;
  }

  @Override
  public Map<OSHDBRole, OSMRole> lookupRole(Set<? extends OSHDBRole> roles) {
    var map = Maps.<OSHDBRole, OSMRole>newHashMapWithExpectedSize(roles.size());
    roles.forEach(role -> map.put(role, lookupRole.get(role)));
    return map;
  }

  private OSHDBTag translate(OSMTag tag){
    return translate.computeIfAbsent(tag, this::tag);
  }

  private OSHDBTag tag(OSMTag tag) {
    var key = string(tag.getKey());
    var val = string(tag.getValue());
    var oshdb = new OSHDBTag(key,val);
    lookup.put(oshdb, tag);
    return oshdb;
  }

  private OSHDBRole translate(OSMRole role) {
    return translateRole.computeIfAbsent(role, this::role);
  }

  private OSHDBRole role(OSMRole role) {
    var oshdb = OSHDBRole.of(string(role.toString()));
    lookupRole.put(oshdb, role);
    return oshdb;
  }

  private int string(String string) {
     return stringIds.computeIfAbsent(string, x -> nextStringId.getAndIncrement());
  }

}
