/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class Filters {
  private final OSHDB oshdb;

  private final BoundingBox bbox = new BoundingBox(8.651133,8.6561,49.387611,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps(2014, 2014, 1, 1);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps(2014, 2015, 1, 1);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  private final double DELTA = 1e-8;

  public Filters() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data;ACCESS_MODE_DATA=r");
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
        .areaOfInterest(bbox.getGeometry())
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  // filter: osm tags

  @Test
  public void tagKey() throws Exception {
    SortedMap<OSMType, Integer> result = createMapReducerOSMEntitySnapshot()
        .filterByTagKey("building")
        .areaOfInterest(bbox.getGeometry())
        .timestamps(timestamps1)
        .countAggregate(snapshot -> snapshot.getEntity().getType());
    assertEquals(1, result.get(OSMType.RELATION).intValue());
    assertEquals(42, result.get(OSMType.WAY).intValue());
  }

  @Test
  public void tagKeyValue() throws Exception {
    Integer result = createMapReducerOSMEntitySnapshot()
        .filterByTagValue("highway", "residential")
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox.getGeometry())
        .timestamps(timestamps1)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  public void tagMultiple() throws Exception {
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .filterByTagKey("name")
        .filterByTagKey("highway")
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox.getGeometry())
        .timestamps(timestamps1)
        .uniq(snapshot -> {
          int[] tags = snapshot.getEntity().getTags();
          for (int i=0; i<tags.length; i+=2)
            if (tags[i] == 6 /*name*/) return tags[i+1];
          return -1; // cannot actually happen (since we query only snapshots with a name, but needed to make Java's compiler happy
        });
    assertEquals(2, result.size());
  }

  // custom filter

  @Test
  public void custom() throws Exception {
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .filter(entity -> entity.getVersion() > 2)
        .osmTypes(OSMType.WAY)
        .areaOfInterest(bbox.getGeometry())
        .timestamps(timestamps1)
        .uniq(snapshot -> snapshot.getEntity().getVersion());
    assertEquals(2, result.size());
  }


}
