package org.heigit.bigspatialdata.updater.util;

import io.reactivex.Flowable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import org.heigit.bigspatialdata.updater.OSCHandling.OSCDownloader;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.xml.common.CompressionActivator;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeIterator extends IteratorTmpl<ChangeContainer> {
  private static final Logger LOG = LoggerFactory.getLogger(ChangeIterator.class);
  private final Iterator<ReplicationFile> replicationFiles;
  private ReplicationFile currFile;
  private Iterator<ChangeContainer> currChangeIterator;

  public ChangeIterator(Iterable<ReplicationFile> replicationFiles) {
    this.replicationFiles = replicationFiles.iterator();
  }

  @Override
  protected ChangeContainer getNext() throws Exception {
    if (currChangeIterator != null && currChangeIterator.hasNext()) {
      return currChangeIterator.next();
    } else if (replicationFiles.hasNext()) {
      currFile = replicationFiles.next();
      currChangeIterator = ChangeIterator.getChangeContainers(currFile).iterator();
      OSCDownloader.updateState(currFile.state.store());
      currFile.file.delete();
      return this.getNext();
    } else {
      return null;
    }

  }

  private static Iterable<ChangeContainer> getChangeContainers(ReplicationFile replicationFile) throws FileNotFoundException {
    LOG.info(replicationFile.state + " -> " + replicationFile.file);
    return generateChangeFlow(replicationFile.file)
        // .limit(10)

        .blockingIterable();

  }

  /**
   * generate flow for ChangeContainer within a osc file
   */
  private static Flowable<ChangeContainer> generateChangeFlow(File osc) throws FileNotFoundException {
    InputStream inputStream = new CompressionActivator(CompressionMethod.GZip)
        .createCompressionInputStream(new FileInputStream(osc));
    Flowable<ChangeContainer> flow = Flowable.generate(() -> XmlChangeReaderIterator.of(inputStream),
        (reader, emitter) -> {
          if (reader.hasNext()) {
            emitter.onNext(reader.next());
          } else {
            if (reader.hasException()) {
              emitter.onError(reader.getException());
            } else {
              emitter.onComplete();
            }
          }
        }, (reader) -> reader.close());
    return flow;
  }
}
