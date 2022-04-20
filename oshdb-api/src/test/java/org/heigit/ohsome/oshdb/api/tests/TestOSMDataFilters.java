package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests osm data filters.
 */
public class TestOSMDataFilters {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");

  public TestOSMDataFilters() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb);
  }

  // filter: area of interest
  // filter: osm type

  @Test
  public void bbox() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmType(OSMType.NODE)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void polygon() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmType(OSMType.NODE)
        .areaOfInterest(OSHDBGeometryBuilder.getGeometry(bbox))
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void multiPolygon() throws Exception {
    GeometryFactory gf = new GeometryFactory();
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmType(OSMType.NODE)
        .areaOfInterest(gf.createMultiPolygon(new Polygon[] {
            OSHDBGeometryBuilder.getGeometry(bbox)
        }))
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void types() throws Exception {
    Set<OSMType> result;
    // single type
    result = createMapReducerOSMEntitySnapshot()
        .osmType(OSMType.NODE)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .map(snapshot -> snapshot.getEntity().getType())
        .stream().collect(Collectors.toSet());
    assertTrue(result.equals(EnumSet.of(OSMType.NODE)));
    // multiple types
    result = createMapReducerOSMEntitySnapshot()
        .osmType(OSMType.NODE, OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .map(snapshot -> snapshot.getEntity().getType())
        .stream().collect(Collectors.toSet());
    assertTrue(result.equals(EnumSet.of(OSMType.NODE, OSMType.WAY)));
    // multiple types (set)
    result = createMapReducerOSMEntitySnapshot()
        .osmType(EnumSet.of(OSMType.NODE, OSMType.WAY))
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .map(snapshot -> snapshot.getEntity().getType())
        .stream().collect(Collectors.toSet());
    assertTrue(result.equals(EnumSet.of(OSMType.NODE, OSMType.WAY)));
    // empty set
    result = createMapReducerOSMEntitySnapshot()
        .osmType(new HashSet<>())
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .map(snapshot -> snapshot.getEntity().getType())
        .stream().collect(Collectors.toSet());
    assertTrue(result.equals(EnumSet.noneOf(OSMType.class)));
    // called multiple times
    result = createMapReducerOSMEntitySnapshot()
        .osmType(OSMType.NODE)
        .osmType(EnumSet.allOf(OSMType.class))
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .map(snapshot -> snapshot.getEntity().getType())
        .stream().collect(Collectors.toSet());
    assertTrue(result.equals(EnumSet.of(OSMType.NODE)));
  }

  // filter: osm tags

  @Test
  public void tagKey() throws Exception {
    SortedMap<OSMType, Integer> result = createMapReducerOSMEntitySnapshot()
        .osmTag("building")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .aggregateBy(snapshot -> snapshot.getEntity().getType())
        .count();
    assertEquals(1, result.get(OSMType.RELATION).intValue());
    assertEquals(42, result.get(OSMType.WAY).intValue());
  }

  @Test
  public void tagKeyValue() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTag("highway", "residential")
        .osmType(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void tagKeyValues() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTag("highway", Arrays.asList("residential", "unclassified"))
        .osmType(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(5, result.intValue());
  }

  @Test
  public void tagKeyValueRegexp() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTag("highway", Pattern.compile("residential|unclassified"))
        .osmType(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(5, result.intValue());
  }

  @Test
  public void tagList() throws Exception {
    // only tags
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTag(Arrays.asList(
            new OSMTag("highway", "residential"),
            new OSMTag("highway", "unclassified"))
        )
        .osmType(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(5, result.intValue());
    // tags and keys mixed
    result = createMapReducerOSMEntitySnapshot()
        .osmTag(Arrays.asList(
            new OSMTag("highway", "residential"),
            new OSMTag("highway", "unclassified"),
            new OSMTagKey("building"))
        )
        .osmType(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(5 + 42, result.intValue());
  }

  @Test
  public void tagMultiple() throws Exception {
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .osmTag("name")
        .osmTag("highway")
        .osmType(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .uniq(snapshot -> {
          for (OSHDBTag tag : snapshot.getEntity().getTags()) {
            if (tag.getKey() == 6 /* name */) {
              return tag.getValue();
            }
          }
          // cannot actually happen (since we query only snapshots with a name, but needed to make
          // Java's compiler happy
          return -1;
        });
    assertEquals(2, result.size());
  }


  @Test
  public void tagNotExists() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTag("buildingsss")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(0, result.intValue());

    result = createMapReducerOSMEntitySnapshot()
        .osmTag("building", "residentialll")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(0, result.intValue());

    result = createMapReducerOSMEntitySnapshot()
        .osmTag("buildingsss", "residentialll")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(0, result.intValue());
  }

  // custom filter

  @Test
  public void custom() throws Exception {
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .osmEntityFilter(entity -> entity.getVersion() > 2)
        .osmType(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .uniq(snapshot -> snapshot.getEntity().getVersion());
    assertEquals(4, result.stream().max(Comparator.reverseOrder()).orElse(-1).intValue());
  }


}
