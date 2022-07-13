package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

abstract class TestMapNewReducer {
  final OSHDBDatabase oshdb;
  OSHDBJdbc keytables = null;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps6 = new OSHDBTimestamps("2010-01-01", "2015-01-01",
      OSHDBTimestamps.Interval.YEARLY);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestMapNewReducer(OSHDBDatabase oshdb) {
    this.oshdb = oshdb;
  }

  protected OSMContributionView createMapReducerOSMContribution() throws Exception {
    var view = new OSMContributionView(oshdb, keytables);
    return view
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void testOSMContributionView() throws Exception {
    // simple query
    var result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .map(OSMContribution::getContributorUserId)
        .uniq();

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "flatMap"
    result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .map(OSMContribution::getContributorUserId)
        .filter(uid -> uid > 0)
        .uniq();

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "groupByEntity"
    assertEquals(7, createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .groupByEntity()
        .map(List::size)
        .reduce(Agg::sumInt));

    // "groupByEntity"
    assertEquals(7, createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .groupByEntity(s -> Stream.of(s.count()))
        .reduce(Agg::sumLong));
  }

}
