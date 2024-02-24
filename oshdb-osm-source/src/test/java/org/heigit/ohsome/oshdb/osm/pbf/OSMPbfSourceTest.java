package org.heigit.ohsome.oshdb.osm.pbf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.TagTranslator;
import org.heigit.ohsome.oshdb.mock.MockTranslator;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMRole;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.junit.jupiter.api.Test;

class OSMPbfSourceTest {

  private static final Path testResources = Paths.get("src", "test", "resources");
  private static final Path SAMPLE_PBF = testResources.resolve("sample.pbf");

  @Test
  void openSource() {
    var pbf = testResources.resolve("sample.pbf");
    assertTrue(Files.exists(pbf));
  }

  @Test
  void entities() {
    var source = new OSMPbfSource(SAMPLE_PBF);
    var tagTranslator = new MockTranslator();
    var count = source.entities(tagTranslator).limitRate(10).count().block();
    assertEquals(339, count);

    var entities = new EnumMap<OSMType, Map<Long, OSMEntity>>(OSMType.class);

    source.entities(tagTranslator)
        .bufferUntilChanged(OSMEntity::getType)
        .doOnNext(list -> {
          var type = list.get(0).getType();
          var map = entities.computeIfAbsent(type, x -> new HashMap<>());
          list.forEach(entity -> map.put(entity.getId(), entity));
        })
        .then().block();
    assertEquals(3, entities.size());

    OSMEntity sample;

    sample = entities.get(OSMType.NODE).get(647105170L);
    assertNotNull(sample);
    assertEquals(2, sample.getVersion());
    assertEquals(4210769, sample.getChangesetId());
    assertEquals(1269340001, sample.getEpochSecond());
    assertEquals(234999, sample.getUserId());
    assertEquals(-2344645, ((OSMNode) sample).getLon());
    assertEquals(517635905, ((OSMNode) sample).getLat());
    assertEquals(0, sample.getTags().size());

    sample = entities.get(OSMType.WAY).get(49161822L);
    assertNotNull(sample);
    assertEquals(1, sample.getVersion());
    assertEquals(3754726, sample.getChangesetId());
    assertEquals(1264885069, sample.getEpochSecond());
    assertEquals(508, sample.getUserId());
    assertThat(Set.copyOf(tagTranslator.lookupTag(sample.getTags()).values()))
        .hasSize(2)
        .containsAll(List.of(
            new OSMTag("highway", "residential"),
            new OSMTag("name", "Worcester Road")
        ));
    assertThat(((OSMWay) sample).getMembers())
        .hasSize(5)
        .containsSequence(
            new OSMMember(30983851, OSMType.NODE, -1),
            new OSMMember(623624257, OSMType.NODE, -1),
            new OSMMember(623624154, OSMType.NODE, -1),
            new OSMMember(623624259, OSMType.NODE, -1),
            new OSMMember(623624261, OSMType.NODE, -1));

    sample = entities.get(OSMType.RELATION).get(31640L);

    assertNotNull(sample);
    assertEquals(81, sample.getVersion());
    assertEquals(11640673, sample.getChangesetId());
    assertEquals(1337419064, sample.getEpochSecond());
    assertEquals(24119, sample.getUserId());
    assertThat(Set.copyOf(tagTranslator.lookupTag(sample.getTags()).values()))
        .hasSize(5)
        .containsAll(List.of(
            new OSMTag("type", "route"),
            new OSMTag("ref", "61"),
            new OSMTag("route", "bicycle"),
            new OSMTag("network", "ncn"),
            new OSMTag("name", "NCN National Route 61")
        ));

    assertThat(((OSMRelation) sample).getMembers())
        .hasSize(234)
        .contains(
            new OSMMember(25896435, OSMType.WAY, roleIdOf("forward", tagTranslator)),
            new OSMMember(121267847, OSMType.WAY, roleIdOf("", tagTranslator)));
  }

  private int roleIdOf(String role, TagTranslator translator) {
    var osm = new OSMRole(role);
    return translator.getOSHDBRoleOf(Set.of(osm)).get(osm).getId();
  }

}