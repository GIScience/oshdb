package org.heigit.bigspatialdata.oshdb.api.tests;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests osm data filters.
 */
public class TestOSMDataFilters {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8.651133,49.387611,8.6561,49.390513);
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
          int[] tags = snapshot.getEntity().getRawTags();
          for (int i=0; i<tags.length; i+=2)
            if (tags[i] == 6 /*name*/) return tags[i+1];
          return -1; // cannot actually happen (since we query only snapshots with a name, but needed to make Java's compiler happy
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
