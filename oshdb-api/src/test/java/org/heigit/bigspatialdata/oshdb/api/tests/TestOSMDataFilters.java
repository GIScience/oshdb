/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Database;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.junit.Test;

import java.util.*;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestOSMDataFilters {
  private final OSHDB_Database oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8.651133,49.387611,8.6561,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-01-01");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  private final double DELTA = 1e-8;

  public TestOSMDataFilters() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb);
  }
  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb);
  }

  // filter: area of interest
  // filter: osm type

  @Test
  public void bbox() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTypes(OSMType.NODE)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void polygon() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTypes(OSMType.NODE)
        .areaOfInterest(OSHDBGeometryBuilder.getGeometry(bbox))
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void multiPolygon() throws Exception {
    GeometryFactory gf = new GeometryFactory();
    Integer result = createMapReducerOSMEntitySnapshot()
        .osmTypes(OSMType.NODE)
        .areaOfInterest(gf.createMultiPolygon(new Polygon[] {
            OSHDBGeometryBuilder.getGeometry(bbox)
        }))
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  // filter: osm tags

  @Test
  public void tagKey() throws Exception {
    SortedMap<OSMType, Integer> result = createMapReducerOSMEntitySnapshot()
        .where("building")
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
        .where("highway", "residential")
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void tagKeyValues() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .where("highway", Arrays.asList("residential", "unclassified"))
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(5, result.intValue());
  }

  @Test
  public void tagKeyValueRegexp() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .where("highway", Pattern.compile("residential|unclassified"))
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(5, result.intValue());
  }

  @Test
  public void tagList() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .where(Arrays.asList(
            new ImmutablePair<>("highway", "residential"),
            new ImmutablePair<>("highway", "unclassified"))
        )
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(5, result.intValue());
  }

  @Test
  public void tagMultiple() throws Exception {
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .where("name")
        .where("highway")
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .uniq(snapshot -> {
          int[] tags = snapshot.getEntity().getTags();
          for (int i=0; i<tags.length; i+=2)
            if (tags[i] == 6 /*name*/) return tags[i+1];
          return -1; // cannot actually happen (since we query only snapshots with a name, but needed to make Java's compiler happy
        });
    assertEquals(2, result.size());
  }


  @Test
  public void tagNotExists() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .where("buildingsss")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(0, result.intValue());

    result = createMapReducerOSMEntitySnapshot()
        .where("building", "residentialll")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(0, result.intValue());

    result = createMapReducerOSMEntitySnapshot()
        .where("buildingsss", "residentialll")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .count();
    assertEquals(0, result.intValue());
  }

  // custom filter

  @Test
  public void custom() throws Exception {
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .where(entity -> entity.getVersion() > 2)
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .uniq(snapshot -> snapshot.getEntity().getVersion());
    assertEquals(4, result.stream().max(Comparator.reverseOrder()).orElse(-1).intValue());
  }


}
