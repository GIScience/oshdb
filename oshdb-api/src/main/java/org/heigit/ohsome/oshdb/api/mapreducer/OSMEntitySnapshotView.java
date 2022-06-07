package org.heigit.ohsome.oshdb.api.mapreducer;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

/**
 * Returns the state of OSM elements at specific given points in time.
 */
public class OSMEntitySnapshotView extends OSHDBView<OSMEntitySnapshot> {
  private OSMEntitySnapshotView() {}

  public static OSMEntitySnapshotView view() {
    return new OSMEntitySnapshotView();
  }

  @Override
  public MapReducer<OSMEntitySnapshot> on(OSHDBDatabase oshdb) {
    return oshdb.createMapReducer(this);
  }

  @Override
  public ViewType type() {
    return ViewType.SNAPSHOT;
  }
}
