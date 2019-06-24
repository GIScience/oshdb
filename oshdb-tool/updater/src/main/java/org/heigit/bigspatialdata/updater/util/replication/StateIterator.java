package org.heigit.bigspatialdata.updater.util.replication;

import java.net.URL;
import org.heigit.bigspatialdata.updater.util.IteratorTmpl;
import org.openstreetmap.osmosis.replication.common.ReplicationState;
import org.openstreetmap.osmosis.replication.common.ServerStateReader;

/**
 * An Iterator-Class to iterate through osmium-replication-states.
 */
public class StateIterator extends IteratorTmpl<ReplicationState> {

  private final URL baseUrl;
  private final ReplicationState endState;
  private final ServerStateReader serverStateReader;
  private ReplicationState state;

  /**
   * Create a new StateIterator.
   *
   * @param baseUrl The base url to be requested (e.g.
   * https://planet.openstreetmap.org/replication/day/)
   * @param startState The state to start replicating from
   */
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
