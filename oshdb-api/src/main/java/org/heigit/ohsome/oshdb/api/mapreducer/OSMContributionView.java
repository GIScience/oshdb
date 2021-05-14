package org.heigit.ohsome.oshdb.api.mapreducer;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;

/**
 * Returns all modifications to OSM elements within a given time period.
 */
public class OSMContributionView {
  private OSMContributionView() {}

  public static MapReducer<OSMContribution> on(OSHDBDatabase oshdb) {
    return oshdb.createMapReducer(OSMContribution.class);
  }
}
