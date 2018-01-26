package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.cli.TransformArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.SortedLong2LongMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.reactive.MyLambdaSubscriber;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;
import org.reactivestreams.Publisher;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;

public class Transform {

  private final long maxMemory;
  private Path workDirectory = Paths.get(".");

  private Transform(long maxMemory) {
    this.maxMemory = maxMemory;
  }

  public static Transform withMaxMemory(long availableMemory) {
    return new Transform(availableMemory);
  }
  

  public Transform withWorkDirectory(Path workDirectory) {
    this.workDirectory = workDirectory;
    return this;
  }
  
  public TagToIdMapper getTagToIdMapper() throws FileNotFoundException, IOException{
    return TransformerTagRoles.getTagToIdMapper(workDirectory);
  }
  public RoleToIdMapper getRoleToIdMapper() throws FileNotFoundException, IOException {
    return TransformerTagRoles.getRoleToIdMapper(workDirectory);
  }
  
  public void transformNodes(OsmPbfMeta pbfMeta,int maxZoom, TagToIdMapper tag2Id, int workerId, int workerTotal)  throws IOException {
    final Transformer transformer = new TransformerNode(maxMemory,maxZoom, workDirectory, tag2Id);
    Flowable<List<Entity>> flow = RxOshPbfReader //
        .readOsh(pbfMeta.pbf, pbfMeta.nodeStart, pbfMeta.nodeEnd, pbfMeta.nodeEnd) //
        .map(osh -> osh.getVersions());
    subscribe(flow, transformer::transform, transformer::error,transformer::complete);
  }

 

  public void transformWays(OsmPbfMeta pbfMeta,int maxZoom, TagToIdMapper tag2Id,SortedLong2LongMap node2cell, int workerId, int workerTotal) throws IOException {
    final Transformer transformer = new TransformerWay(maxMemory,maxZoom, workDirectory, tag2Id, node2cell);
    Flowable<List<Entity>> flow = RxOshPbfReader //
        .readOsh(pbfMeta.pbf, pbfMeta.wayStart, pbfMeta.wayEnd, pbfMeta.wayEnd) //
        .map(osh -> osh.getVersions());
    subscribe(flow, transformer::transform, transformer::error,transformer::complete);

  }

  public void transformRelations(OsmPbfMeta pbfMeta,int maxZoom, TagToIdMapper tag2Id, RoleToIdMapper role2Id,SortedLong2LongMap node2cell, SortedLong2LongMap way2cell, int workerId, int workerTotal) throws IOException {
    final Transformer transformer = new TransformerRelation(maxMemory,maxZoom, workDirectory, tag2Id,role2Id, node2cell,way2cell);
    Flowable<List<Entity>> flow = RxOshPbfReader //
        .readOsh(pbfMeta.pbf, pbfMeta.relationStart, pbfMeta.relationEnd, pbfMeta.relationEnd) //
        .map(osh -> osh.getVersions());
    subscribe(flow, transformer::transform, transformer::error,transformer::complete);

  }
  
  private static <T> void subscribe(Publisher<? extends T> o, final Consumer<? super T> onNext,
      final Consumer<? super Throwable> onError, final Action onComplete) {
    ObjectHelper.requireNonNull(onNext, "onNext is null");
    ObjectHelper.requireNonNull(onError, "onError is null");
    ObjectHelper.requireNonNull(onComplete, "onComplete is null");
    FlowableBlockingSubscribe.subscribe(o, new MyLambdaSubscriber<T>(onNext, onError, onComplete, 1L));
  }

  public static void transform(TransformArgs config) throws Exception{
    
    final long MB = 1024L*1024L;
    final long GB = 1024L * MB;
    
    
    final Path pbf = config.pbf;
    final Path workDir = config.common.workDir;

    final String step = config.step;
    final int maxZoom = config.maxZoom;
    boolean overwrite = config.overwrite;

    int worker = config.distribute.worker;
    int workerTotal = config.distribute.totalWorkers;
    if (worker >= workerTotal)
      throw new IllegalArgumentException("worker must be lesser than totalWorker!");
    if(workerTotal > 1 && (step.startsWith("a")))
      throw new IllegalArgumentException("step all with totalWorker > 1 is not allwod use step (node,way or relation)");
    
    final long availableMemory = SizeEstimator.estimateAvailableMemory() - 1*GB; // reserve 1GB for parsing 
    
    System.out.println("Transform:");
    System.out.println("avaliable memory: "+availableMemory);
    
    final Transform transform = Transform.withMaxMemory(availableMemory).withWorkDirectory(workDir);
    final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf); 
    
    final TagToIdMapper tag2Id = transform.getTagToIdMapper();
    
        
    if(step.startsWith("a") || step.startsWith("n")){
      long maxMemory = availableMemory - tag2Id.estimatedSize();
      if(maxMemory < 100*MB)
        System.out.println("warning: only 100MB memory left for transformation! Increase heapsize -Xmx if possible");
      if(maxMemory < 1*MB)
        throw new Exception("to few memory left for transformation. You need to increase JVM heapsize -Xmx for transforming");
      
      System.out.println("maxMemory for transformation: "+maxMemory);
      Transform.withMaxMemory(maxMemory).withWorkDirectory(workDir).transformNodes(pbfMeta,maxZoom, tag2Id, worker, workerTotal);
    }

    if (step.startsWith("a")||step.startsWith("w")) {
      final SortedLong2LongMap node2Cell = new SortedLong2LongMap(workDir.resolve("transform_idToCell_" + "node"), 1*GB);
      long maxMemory = availableMemory - tag2Id.estimatedSize() - 1*GB;
      if(maxMemory < 100*MB)
        System.out.println("warning: only 100MB memory left for transformation! Increase heapsize -Xmx if possible");
      if(maxMemory < 1*MB)
        throw new Exception("to few memory left for transformation. You need to increase JVM heapsize -Xmx for transforming");
      
      System.out.println("maxMemory for transformation: "+maxMemory);
      Transform.withMaxMemory(maxMemory).withWorkDirectory(workDir).transformWays(pbfMeta,maxZoom, tag2Id,node2Cell, worker, workerTotal);
    }

    if (step.startsWith("a")||step.startsWith("r")) {
      final RoleToIdMapper role2Id = Transform.withMaxMemory(availableMemory).withWorkDirectory(workDir).getRoleToIdMapper();
      final SortedLong2LongMap node2Cell = new SortedLong2LongMap(workDir.resolve("transform_idToCell_" + "node"), 1*GB);
      final SortedLong2LongMap way2Cell = new SortedLong2LongMap(workDir.resolve("transform_idToCell_" + "way"), 1*GB);
      
      long maxMemory = availableMemory - tag2Id.estimatedSize() - role2Id.estimatedSize() - 2*GB;
      if(maxMemory < 100*MB)
        System.out.println("warning: only 100MB memory left for transformation! Increase heapsize -Xmx if possible");
      if(maxMemory < 1*MB)
        throw new Exception("to few memory left for transformation. You need to increase JVM heapsize -Xmx for transforming");
      
      System.out.println("maxMemory for transformation: "+maxMemory);
      Transform.withMaxMemory(maxMemory).withWorkDirectory(workDir).transformRelations(pbfMeta,maxZoom, tag2Id, role2Id, node2Cell, way2Cell, worker, workerTotal);
    }
    
    
  }
  
  public static void main(String[] args) throws Exception {
    TransformArgs config = new TransformArgs();
    JCommander jcom = JCommander.newBuilder().addObject(config).build();

    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      System.out.println(e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();
      return;
    }
    if (config.common.help) {
      jcom.usage();
      return;
    }
    transform(config);
  }
  
}
