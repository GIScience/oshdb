package org.heigit.ohsome.oshdb.api.mapreducer;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;

/**
 * Returns all modifications to OSM elements within a given time period.
 */
public class OSMContributionView extends OSHDBView<OSMContribution> {
  private OSMContributionView() {}

  public static OSMContributionView view() {
    return new OSMContributionView();
  }

  @Override
  public MapReducer<OSMContribution> on(OSHDBDatabase oshdb) {
    return oshdb.createMapReducer(this);
  }

  @Override
  public ViewType type() {
    return ViewType.CONTRIBUTION;
  }
}
