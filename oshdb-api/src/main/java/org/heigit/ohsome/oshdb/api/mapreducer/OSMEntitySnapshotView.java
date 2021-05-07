package org.heigit.ohsome.oshdb.api.mapreducer;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshot;

public class OSMEntitySnapshotView {
  private OSMEntitySnapshotView() {}

  public static MapReducer<OSMEntitySnapshot> on(OSHDBDatabase oshdb) {
    return oshdb.<OSMEntitySnapshot>createMapReducer(OSMEntitySnapshot.class);
  }
}
