package org.heigit.bigspatialdata.oshdb.updater;

import java.io.File;

import org.openstreetmap.osmosis.replication.common.ReplicationState;

public class ReplicationFile {
	public final ReplicationState state;
	public final File file;

	public ReplicationFile(ReplicationState state, File file) {
		this.state = state;
		this.file = file;
	}
}
