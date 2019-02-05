package org.heigit.bigspatialdata.updater.util;

import java.net.URL;
import org.openstreetmap.osmosis.replication.common.ReplicationState;
import org.openstreetmap.osmosis.replication.common.ServerStateReader;

public class StateIterator extends IteratorTmpl<ReplicationState> {
  private final URL baseUrl;
  private final ServerStateReader serverStateReader;
  private final ReplicationState endState;
  private ReplicationState state;

  public StateIterator(URL baseUrl, ReplicationState startState) {
    this.baseUrl = baseUrl;
    this.serverStateReader = new ServerStateReader();
    this.endState = serverStateReader.getServerState(baseUrl);
    this.state = startState;
  }

  @Override
  protected ReplicationState getNext() throws Exception {
    if (state.getSequenceNumber() < endState.getSequenceNumber()) {
      final long sequenceNumber = state.getSequenceNumber() + 1;
      state = serverStateReader.getServerState(baseUrl, sequenceNumber);
    } else {
      return null;
    }
    return state;
  }
}
