package org.heigit.ohsome.oshdb.helpers.db;

import java.util.Properties;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;

public class OSHDBConnection {

  private final Properties props;
  private final OSHDBDatabase oshdb;
  private final TagTranslator tagTranslator;

  public OSHDBConnection(Properties props, OSHDBDatabase oshdb)
      throws OSHDBKeytablesNotFoundException {
    this.props = props;
    this.oshdb = oshdb;
    this.tagTranslator = oshdb.getTagTranslator();
  }

  public MapReducer<OSMContribution> getContributionView() {
    return OSMContributionView.on(oshdb);
  }

  public MapReducer<OSMEntitySnapshot> getSnapshotView() {
    return OSMEntitySnapshotView.on(oshdb);
  }

  public Properties getProps() {
    return props;
  }

  public OSHDBDatabase getOSHDB() {
    return oshdb;
  }

  public TagTranslator getTagTranslator() {
    return tagTranslator;
  }
}
