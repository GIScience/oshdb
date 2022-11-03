package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampIllegalArgumentException;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test aggregate by custom index method of the OSHDB API.
 */
class TestMapReducerTimestamps {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);

  TestMapReducerTimestamps() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView
        .on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView
        .on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void testInvalidTimestamps() {
    assertThrows(DateTimeParseException.class, this::invalidTimestamps1);
    assertThrows(OSHDBTimestampIllegalArgumentException.class, this::invalidTimestamps2);
  }

  private void invalidTimestamps1() throws Exception {
    // contribution view query
    var ignored = createMapReducerOSMContribution()
        .timestamps("invalid1", "invalid2")
        .map(OSMContribution::getContributorUserId)
        .uniq();
    // snapshot view query
    var ignored2 = createMapReducerOSMEntitySnapshot()
        .timestamps("invalid")
        .count();
  }

  @SuppressWarnings("UnusedAssignment")
  private void invalidTimestamps2() throws Exception {
    // invalid time zone
    var ignored = createMapReducerOSMEntitySnapshot()
        .timestamps("2020-01-01T00:00:00+00")
        .count();
    // invalid sign
    ignored = createMapReducerOSMEntitySnapshot()
        .timestamps("-2020-01-01T00:00:00Z")
        .count();
  }
}
