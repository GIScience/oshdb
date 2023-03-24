package org.heigit.ohsome.oshdb.source;

import static java.util.Arrays.stream;
import static org.heigit.ohsome.oshdb.osm.OSM.node;
import static org.heigit.ohsome.oshdb.osm.OSM.relation;
import static org.heigit.ohsome.oshdb.osm.OSM.way;
import static org.heigit.ohsome.oshdb.util.flux.FluxUtil.entryToTuple;
import static org.heigit.ohsome.oshdb.util.flux.FluxUtil.mapT2;
import static org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator.TranslationOption.ADD_MISSING;
import static reactor.core.publisher.Flux.fromIterable;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMRole;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

public class SourceUtil {


  private SourceUtil() {
    // utility class
  }

  public static Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities(Map<OSMType, List<OSMEntity>> entities,
      Map<OSHDBTag, OSMTag> tags, Map<Integer, OSMRole> roles, TagTranslator tagTranslator) {
    var tagsMapping = tagsMapping(tagTranslator, tags);
    var rolesMapping = rolesMapping(tagTranslator, roles);
    return fromIterable(entities.entrySet())
        .map(entryToTuple())
        .map(mapT2(f -> fromIterable(f).map(osm -> map(osm, tagsMapping, rolesMapping))));
  }

  private static synchronized Map<OSHDBTag, OSHDBTag> tagsMapping(TagTranslator tagTranslator,
      Map<OSHDBTag, OSMTag> tags) {
    var tagsTranslated = tagTranslator.getOSHDBTagOf(tags.values(), ADD_MISSING);
    var tagsMapping = Maps.<OSHDBTag, OSHDBTag>newHashMapWithExpectedSize(tags.size());
    tags.forEach((oshdb, osm) -> tagsMapping.put(oshdb, tagsTranslated.get(osm)));
    return tagsMapping;
  }

  private static synchronized Map<Integer, Integer> rolesMapping(TagTranslator tagTranslator,
      Map<Integer, OSMRole> roles) {
    var rolesTranslated = tagTranslator.getOSHDBRoleOf(roles.values(), ADD_MISSING);
    var rolesMapping = Maps.<Integer, Integer>newHashMapWithExpectedSize(roles.size());
    roles.forEach((oshdb, osm) -> rolesMapping.put(oshdb, rolesTranslated.get(osm).getId()));
    return rolesMapping;
  }

  public static int version(int version, boolean visible) {
    return visible ? version : -version;
  }

  private static OSMEntity map(OSMEntity osm, Map<OSHDBTag, OSHDBTag> tagsMapping,
      Map<Integer, Integer> rolesMapping) {
    var tags = osm.getTags().stream().map(tagsMapping::get).sorted().toList();
    if (osm instanceof OSMNode node) {
      return node(osm.getId(), version(osm.getVersion(), osm.isVisible()), osm.getEpochSecond(),
          osm.getChangesetId(), osm.getUserId(), tags, node.getLon(), node.getLat());
    } else if (osm instanceof OSMWay way) {
      return way(osm.getId(), version(osm.getVersion(), osm.isVisible()), osm.getEpochSecond(),
          osm.getChangesetId(), osm.getUserId(), tags, way.getMembers());
    } else if (osm instanceof OSMRelation relation) {
      var members = stream(relation.getMembers()).map(
              mem -> new OSMMember(mem.getId(), mem.getType(), rolesMapping.get(mem.getRole().getId())))
          .toArray(OSMMember[]::new);
      return relation(osm.getId(), version(osm.getVersion(), osm.isVisible()),
          osm.getEpochSecond(), osm.getChangesetId(), osm.getUserId(), tags, members);
    } else {
      throw new IllegalStateException();
    }
  }

}
