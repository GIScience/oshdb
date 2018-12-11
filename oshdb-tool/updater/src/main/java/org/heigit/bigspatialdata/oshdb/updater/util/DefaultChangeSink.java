package org.heigit.bigspatialdata.oshdb.updater.util;

import java.util.Map;

import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;

public interface DefaultChangeSink extends ChangeSink {

	default void initialize(Map<String, Object> metaData) {
		//no op
	}

	default void complete() {
		// no op
	}

	default void close() {
		// no op
	}
}
