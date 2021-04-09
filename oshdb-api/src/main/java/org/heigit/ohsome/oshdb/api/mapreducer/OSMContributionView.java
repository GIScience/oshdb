package org.heigit.ohsome.oshdb.api.mapreducer;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.object.OSMContribution;

public class OSMContributionView {
  private OSMContributionView() {}

  public static MapReducer<OSMContribution> on(OSHDBDatabase oshdb) {
    return oshdb.<OSMContribution>createMapReducer(OSMContribution.class);
  }
}
