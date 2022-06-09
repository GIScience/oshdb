package org.heigit.ohsome.oshdb.api.mapreducer.view;

import com.google.common.collect.Streams;
import java.io.IOException;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.improve.OSHDBJdbcImprove;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.json.simple.parser.ParseException;

public class OSMEntityView extends OSHDBView<OSMEntity> {
  private OSMEntityView(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    super(oshdb, keytables);
  }

  @Override
  public MapReducer<OSMEntity> view() throws OSHDBException {
    return oshdb.createMapReducer(this);
  }

  public static OSMEntityView on(OSHDBDatabase oshdb, OSHDBJdbc keytables) {
    return new OSMEntityView(oshdb, keytables);
  }

  private MapReducer<OSMEntity> improve(OSHDBJdbcImprove oshdb) throws IOException, ParseException {
    return oshdb.createMapReducerImprove(this)
        .flatMap(osh -> Streams.stream(osh.getVersions()).map(OSMEntity.class::cast));
  }

  @Override
  public ViewType type() {
    return null;
  }
}
