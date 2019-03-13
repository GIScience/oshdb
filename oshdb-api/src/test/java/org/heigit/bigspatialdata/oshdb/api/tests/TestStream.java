package org.heigit.bigspatialdata.oshdb.api.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.ConcurrentHashMap;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBUpdate;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStream {

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8.651133,49.387611,8.6561,49.390513);
  private final OSHDBDatabase oshdb;
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);
  private final OSHDBTimestamps timestampsUpdate = new OSHDBTimestamps("2010-01-01", "2020-12-01", OSHDBTimestamps.Interval.MONTHLY);

  public TestStream() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data").multithreading(false);
  }

  @Test
  public void testForEach() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .stream()
        .forEach(contribution -> {
          result.put(contribution.getEntityAfter().getId(), true);
        });
    assertEquals(42, result.entrySet().size());
  }

  @Test
  public void testForEachAggregatedByTimestamp() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .stream()
        .forEach(entry ->
          result.put(entry.getValue().getEntityAfter().getId(), true)
        );
    assertEquals(42, result.entrySet().size());
  }
  
  @Test
  public void testForEachGroupedById() throws Exception {
    ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
    this.createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .groupByEntity()
        .map(contributions -> contributions.get(0).getEntityAfter().getId())
        .stream()
        .forEach(id -> result.put(id, true));
    assertEquals(42, result.entrySet().size());
  }
  
  @Test
  public void testForEachUpdate() throws Exception {
      try (Connection conn = DriverManager.getConnection(
          "jdbc:h2:./src/test/resources/update-test-data");) {

        ConcurrentHashMap<Long, Boolean> result = new ConcurrentHashMap<>();
        this.createUpdateMapReducerOSMContribution()
            .updates(new OSHDBUpdate(conn))
            .timestamps(timestampsUpdate)
            .stream()
            .forEach(contribution -> {
              result.put(contribution.getEntityAfter().getId(), true);
            });
        assertTrue("The node from update-db is missing.",result.keySet().contains(6286318977L));
      }
  }
  
  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmType(OSMType.WAY).osmTag("building", "yes").areaOfInterest(bbox);
  }
  
    private MapReducer<OSMContribution> createUpdateMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmType(OSMType.NODE);
  }

}
