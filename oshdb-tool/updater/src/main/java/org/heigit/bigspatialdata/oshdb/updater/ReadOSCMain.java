package org.heigit.bigspatialdata.oshdb.updater;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.openstreetmap.osmosis.core.OsmosisConstants;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.core.util.FileBasedLock;
import org.openstreetmap.osmosis.core.util.PropertiesPersister;
import org.openstreetmap.osmosis.osmbinary.file.BlockOutputStream;
import org.openstreetmap.osmosis.replication.common.ReplicationSequenceFormatter;
import org.openstreetmap.osmosis.replication.common.ReplicationState;
import org.openstreetmap.osmosis.replication.common.ServerStateReader;
import org.openstreetmap.osmosis.xml.common.CompressionActivator;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import crosby.binary.osmosis.OsmosisSerializer;
import io.reactivex.Flowable;

public class ReadOSCMain {
  private static final String LOCK_FILE = "download.lock";
  private static final String LOCAL_STATE_FILE = "state.txt";

  private final ReplicationSequenceFormatter sequenceFormatter =
      new ReplicationSequenceFormatter(9, 3);
  private final Path workingDirectory;
  private final PropertiesPersister localStatePersistor;

  private LongBitmapDataProvider newNodes = new Roaring64NavigableMap();
  private LongBitmapDataProvider newWays = new Roaring64NavigableMap();
  private LongBitmapDataProvider newRelations = new Roaring64NavigableMap();
  private LongBitmapDataProvider newWaysWithExistingNodes = new Roaring64NavigableMap();
  private LongBitmapDataProvider newRelationsWithExistingNodes = new Roaring64NavigableMap();
  private LongBitmapDataProvider existingNodes = new Roaring64NavigableMap();
  private LongBitmapDataProvider existingWays = new Roaring64NavigableMap();
  private LongBitmapDataProvider existingRelations = new Roaring64NavigableMap();

  // private EntityContainer entityContainer;
  private List<EntityContainer> newEntitiesNode;
  private List<EntityContainer> newEntitiesWay;
  private List<EntityContainer> newEntitiesRelation;
  private List<EntityContainer> updatedEntitiesNode;
  private List<EntityContainer> updatedEntitiesWay;
  private List<EntityContainer> updatedEntitiesRelation;
  private List<EntityContainer> existingEntitiesNode;
  private List<EntityContainer> existingEntitiesWay;
  private List<EntityContainer> existingEntitiesRelation;
  



  public static void main(String[] args)
      throws ParseException, MalformedURLException, FileNotFoundException {
    final Path workingDirectory = Paths.get(".");
    final URL baseUrl = new URL("https://planet.openstreetmap.org/replication/minute/");


    final FileBasedLock fileLock = new FileBasedLock(workingDirectory.resolve(LOCK_FILE).toFile());
    try {
      fileLock.lock();
      new ReadOSCMain(workingDirectory).run(baseUrl);
      fileLock.unlock();
    } finally {
      fileLock.close();
    }
  }

  public void run(URL baseUrl) throws FileNotFoundException {
    final ServerStateReader serverStateReader = new ServerStateReader();
    final ReplicationState serverState = serverStateReader.getServerState(baseUrl);
    System.out.println("latest server state form " + baseUrl);
    System.out.println(serverState);
    System.out.println();



    final ReplicationState localState;
    if (!localStatePersistor.exists()) {
      System.out.println("no prior state.txt exist in " + workingDirectory
          + " starting from latest server state now");
      localState = serverStateReader.getServerState(baseUrl, serverState.getSequenceNumber() - 1);
      localStatePersistor.store(localState.store());
    } else {
      System.out.println("latest local state");
      localState = new ReplicationState(localStatePersistor.loadMap());
      System.out.println(localState);
    }

    System.out.println();


    newEntitiesNode = new ArrayList<>();
    newEntitiesWay = new ArrayList<>();
    newEntitiesRelation = new ArrayList<>();
    updatedEntitiesNode = new ArrayList<>();
    updatedEntitiesWay = new ArrayList<>();
    updatedEntitiesRelation = new ArrayList<>();
    existingEntitiesNode = new ArrayList<>();
    existingEntitiesWay = new ArrayList<>();
    existingEntitiesRelation = new ArrayList<>();

    File pbfOutFile1 = new File("output_newEntities.pbf");

    BlockOutputStream output1 = new BlockOutputStream(new FileOutputStream(pbfOutFile1));
    output1.setCompress("deflate");

    OsmosisSerializer pbf1 = new OsmosisSerializer(output1);
    pbf1.configBatchLimit(8000);

    pbf1.configOmit(true);
    pbf1.setUseDense(true);
    pbf1.configGranularity(100);
    
    File pbfOutFile2 = new File("output_newWaysRelations_with_existing_Nodes.pbf");

    BlockOutputStream output2 = new BlockOutputStream(new FileOutputStream(pbfOutFile2));
    output2.setCompress("deflate");

    OsmosisSerializer pbf2 = new OsmosisSerializer(output2);
    pbf2.configBatchLimit(8000);

    pbf2.configOmit(true);
    pbf2.setUseDense(true);
    pbf2.configGranularity(100);
    
    File pbfOutFile3 = new File("output_existingEntities.pbf");

    BlockOutputStream output3 = new BlockOutputStream(new FileOutputStream(pbfOutFile3));
    output3.setCompress("deflate");

    OsmosisSerializer pbf3 = new OsmosisSerializer(output3);
    pbf3.configBatchLimit(8000);

    pbf3.configOmit(true);
    pbf3.setUseDense(true);
    pbf3.configGranularity(100);



    for (ReplicationFile replicationFile : generateStateFlow(baseUrl, localState)
        .map((ReplicationState state) -> {
          final String fileName =
              sequenceFormatter.getFormattedName(state.getSequenceNumber(), ".osc.gz");
          final File replicationFile = downloadReplicationFile(fileName, baseUrl);
          return new ReplicationFile(state, replicationFile);
        })

        // we could limit how many file we would like to process in this run!
        // just comment out this limit if you don't want it.
        .limit(4)

        .blockingIterable()) {

      System.out.println("processing");
      System.out.println(replicationFile.state + " -> " + replicationFile.file);


      try {

        // Flowable<ChangeContainer> changes = generateChangeFlow(replicationFile.file);
        filterNodes(replicationFile, pbf1, pbf2, pbf3);


        filterWays(replicationFile, pbf1, pbf2, pbf3);

        filterRelations(replicationFile, pbf1, pbf2, pbf3);

        System.out.println(newNodes.getLongCardinality() + " " + newWays.getLongCardinality() + " "
            + newRelations.getLongCardinality() + " "
            + newWaysWithExistingNodes.getLongCardinality() + " "
            + newRelationsWithExistingNodes.getLongCardinality() + " "
            + existingNodes.getLongCardinality() + " " + existingWays.getLongCardinality() + " "
            + existingRelations.getLongCardinality());



      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      localStatePersistor.store(replicationFile.state.store());
      replicationFile.file.delete();
      System.out.println();
    }


    // TODO write out a pbf file
    // BEWARE that we want to write first all nodes, then ways, than relations

    for (EntityContainer entity : newEntitiesNode) {
      pbf1.process(entity);
    }
    for (EntityContainer entity : newEntitiesWay) {
      pbf1.process(entity);
    }
    for (EntityContainer entity : newEntitiesRelation) {
      pbf1.process(entity);
    }

    pbf1.complete();
    pbf1.close();
    
    for (EntityContainer entity : updatedEntitiesNode) {
      pbf2.process(entity);
    }
    for (EntityContainer entity : updatedEntitiesWay) {
      pbf2.process(entity);
    }
    for (EntityContainer entity : updatedEntitiesRelation) {
      pbf2.process(entity);
    }
    
    pbf2.complete();
    pbf2.close();
    
    for (EntityContainer entity : existingEntitiesNode) {
      pbf3.process(entity);
    }
    for (EntityContainer entity : existingEntitiesWay) {
      pbf3.process(entity);
    }
    for (EntityContainer entity : existingEntitiesRelation) {
      pbf3.process(entity);
    }
    
    pbf3.complete();
    pbf3.close();
    System.out.println("finish");
  }

  /**
   * @param change
   */
  private boolean checkWay(Way way) {
    List<WayNode> WayNodes = way.getWayNodes();
    boolean isNew = true;
    for (WayNode wayNode : WayNodes) {
      if (existingNodes.contains(wayNode.getNodeId())) {
        isNew = false;
        break;
      }
    }
    return isNew;
  }

  private void filterNodes(ReplicationFile replicationFile, OsmosisSerializer pbf1, OsmosisSerializer pbf2, OsmosisSerializer pbf3) throws FileNotFoundException {
    for (ChangeContainer change : generateChangeFlow(replicationFile.file)

        // .limit(10)

        .blockingIterable()) {

      if (change.getAction() == ChangeAction.Create
          && change.getEntityContainer().getEntity().getType() == EntityType.Node) {

        newNodes.addLong(change.getEntityContainer().getEntity().getId());
        newEntitiesNode.add(change.getEntityContainer());
      }
      if ((change.getAction() == ChangeAction.Modify || change.getAction() == ChangeAction.Delete)
          && change.getEntityContainer().getEntity().getType() == EntityType.Node) {

        existingNodes.addLong(change.getEntityContainer().getEntity().getId());
        existingEntitiesNode.add(change.getEntityContainer());
      }
    }

  }

  private void filterRelations(ReplicationFile replicationFile, OsmosisSerializer pbf1, OsmosisSerializer pbf2, OsmosisSerializer pbf3) throws FileNotFoundException {
    for (ChangeContainer change : generateChangeFlow(replicationFile.file)

        // .limit(10)

        .blockingIterable()) {


      if (change.getAction() == ChangeAction.Create
          && change.getEntityContainer().getEntity().getType() == EntityType.Relation) {

        boolean isNew = checkRelation((Relation) (change.getEntityContainer().getEntity()));
        if (isNew) {
          newRelations.addLong(change.getEntityContainer().getEntity().getId());
          newEntitiesRelation.add(change.getEntityContainer());
        }
        if (!isNew) {
          newRelationsWithExistingNodes.addLong(change.getEntityContainer().getEntity().getId());
          updatedEntitiesRelation.add(change.getEntityContainer());
        }
      }
      if ((change.getAction() == ChangeAction.Modify || change.getAction() == ChangeAction.Delete)
          && change.getEntityContainer().getEntity().getType() == EntityType.Relation) {
        existingRelations.addLong(change.getEntityContainer().getEntity().getId());
        existingEntitiesRelation.add(change.getEntityContainer());
      }

    }
  }

  /**
   * @param change
   */
  private boolean checkRelation(Relation relation) {
    List<RelationMember> relMembers = relation.getMembers();
    boolean newRelation = true;
    for (RelationMember relationMember : relMembers) {
      if (relationMember.getMemberType() == EntityType.Way
          && (newWaysWithExistingNodes.contains(relationMember.getMemberId()))
          || existingWays.contains(relationMember.getMemberId())) {
        newRelation = false;
        break;
      }
      if (relationMember.getMemberType() == EntityType.Node
          && existingNodes.contains(relationMember.getMemberId())) {
        newRelation = false;
        break;
      }
    }
    return newRelation;
  }


  private void filterWays(ReplicationFile replicationFile, OsmosisSerializer pbf1, OsmosisSerializer pbf2, OsmosisSerializer pbf3) throws FileNotFoundException {
    for (ChangeContainer change : generateChangeFlow(replicationFile.file)

        // .limit(10)

        .blockingIterable()) {


      if (change.getAction() == ChangeAction.Create
          && change.getEntityContainer().getEntity().getType() == EntityType.Way) {

        boolean isNew = checkWay((Way) change.getEntityContainer().getEntity());
        if (!isNew) {
          newWaysWithExistingNodes.addLong(change.getEntityContainer().getEntity().getId());
          updatedEntitiesWay.add(change.getEntityContainer());
        }
        if (isNew) {
          newWays.addLong(change.getEntityContainer().getEntity().getId());
          newEntitiesWay.add(change.getEntityContainer());
        }

      }
      if ((change.getAction() == ChangeAction.Modify || change.getAction() == ChangeAction.Delete)
          && change.getEntityContainer().getEntity().getType() == EntityType.Way) {

        existingWays.addLong(change.getEntityContainer().getEntity().getId());
        existingEntitiesWay.add(change.getEntityContainer());

      }

    }

  }

  public ReadOSCMain(Path workingDirectory) {
    this.workingDirectory = workingDirectory;
    this.localStatePersistor =
        new PropertiesPersister(workingDirectory.resolve(LOCAL_STATE_FILE).toFile());
  }

  /**
   * Download osc file from server
   */
  private File downloadReplicationFile(String fileName, URL baseUrl) {
    URL changesetUrl;
    try {
      changesetUrl = new URL(baseUrl, fileName);
    } catch (MalformedURLException e) {
      throw new OsmosisRuntimeException("The server file URL could not be created.", e);
    }

    try {
      File outputFile;
      // Open an input stream for the changeset file on the server.
      URLConnection connection = changesetUrl.openConnection();
      connection.setReadTimeout(15 * 60 * 1000); // timeout 15 minutes
      connection.setConnectTimeout(15 * 60 * 1000); // timeout 15 minutes
      connection.setRequestProperty("User-Agent", "Osmosis/" + OsmosisConstants.VERSION);

      try (BufferedInputStream source =
          new BufferedInputStream(connection.getInputStream(), 65536)) {
        // Create a temporary file to write the data to.
        outputFile = File.createTempFile("change", null);

        // Open a output stream for the destination file.
        try (BufferedOutputStream sink =
            new BufferedOutputStream(new FileOutputStream(outputFile), 65536)) {
          // Download the file.
          byte[] buffer = new byte[65536];
          for (int bytesRead = source.read(buffer); bytesRead > 0; bytesRead =
              source.read(buffer)) {
            sink.write(buffer, 0, bytesRead);
          }
        }
      }

      return outputFile;

    } catch (IOException e) {
      throw new OsmosisRuntimeException(
          "Unable to read the changeset file " + fileName + " from the server.", e);
    }
  }

  /**
   * generate flow for state retrieved from the server beginning from start state
   */
  private Flowable<ReplicationState> generateStateFlow(URL baseUrl, ReplicationState startState) {
    Flowable<ReplicationState> flow =
        Flowable.generate(() -> new StateIterator(baseUrl, startState), (itr, emitter) -> {
          if (itr.hasNext()) {
            emitter.onNext(itr.next());
          } else {
            if (itr.hasException()) {
              emitter.onError(itr.getException());
            } else {
              emitter.onComplete();
            }
          }
        });
    return flow;
  }

  /**
   * generate flow for ChangeContainer within a osc file
   */
  private Flowable<ChangeContainer> generateChangeFlow(File osc) throws FileNotFoundException {
    InputStream inputStream = new CompressionActivator(CompressionMethod.GZip)
        .createCompressionInputStream(new FileInputStream(osc));
    Flowable<ChangeContainer> flow =
        Flowable.generate(() -> XmlChangeReaderIterator.of(inputStream), (reader, emitter) -> {
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
