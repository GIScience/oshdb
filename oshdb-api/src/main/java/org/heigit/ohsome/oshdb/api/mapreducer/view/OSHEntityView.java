package org.heigit.ohsome.oshdb.api.mapreducer.view;

import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.improve.OSHDBJdbcImprove;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;

public class OSHEntityView extends OSHDBView<OSHEntity> {

  private OSHEntityView(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    super(oshdb, keytables);
  }

  @Override
  public MapReducer<OSHEntity> view() throws OSHDBException {
    return oshdb.createMapReducer(this);
  }

  public static OSHEntityView on(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    return new OSHEntityView(oshdb, keytables);
  }

  private MapReducer<OSHEntity> improve(OSHDBJdbcImprove oshdb) {
    return oshdb.createMapReducerImprove(this);
  }

  @Override
  public ViewType type() {
    return null;
  }
}
