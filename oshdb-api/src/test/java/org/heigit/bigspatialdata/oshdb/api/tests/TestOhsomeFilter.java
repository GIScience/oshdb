package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.junit.Test;

import java.util.Set;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests integration of ohsome-filter library.
 *
 * <p>
 *   Only basic "is it working at all" tests are done, since the library itself has its own set
 *   of unit tests.
 * </p>
 */
public class TestOhsomeFilter {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8.651133,49.387611,8.6561,49.390513);

  public TestOhsomeFilter() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducer() throws Exception {
    return OSMEntitySnapshotView.on(oshdb)
        .areaOfInterest(bbox)
        .timestamps("2014-01-01");
  }

  @Test
  public void testFilterString() throws Exception {
    Integer result = createMapReducer()
        .filter("type:way and building=*")
        .count();

    assertEquals(42, result.intValue());
  }

  @Test
  public void testAggregateFilter() throws Exception {
    SortedMap<OSMType, Integer> result = createMapReducer()
        .filter("(geometry:polygon or geometry:other) and building=*")
        .aggregateBy(x -> x.getEntity().getType())
        .count();

    assertEquals(2, result.entrySet().size());
    assertEquals(42, result.get(OSMType.WAY).intValue());
    assertEquals(1, result.get(OSMType.RELATION).intValue());
  }
}
