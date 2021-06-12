package org.heigit.ohsome.oshdb.api.mapreducer;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

/**
 * Returns the state of OSM elements at specific given points in time.
 */
public class OSMEntitySnapshotView {
  private OSMEntitySnapshotView() {}

  public static MapReducer<OSMEntitySnapshot> on(OSHDBDatabase oshdb) {
    return oshdb.createMapReducer(OSMEntitySnapshot.class);
  }
}
