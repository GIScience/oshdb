package org.heigit.bigspatialdata.updater.util.replication;

import java.util.Map;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;

public interface DefaultChangeSink extends ChangeSink {

  @Override
  default void initialize(Map<String, Object> metaData) {
    //no op
  }

  @Override
  default void complete() {
    // no op
  }

  @Override
  default void close() {
    // no op
  }

}
