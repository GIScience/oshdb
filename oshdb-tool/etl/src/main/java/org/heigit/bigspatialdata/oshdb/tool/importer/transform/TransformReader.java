package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

public class TransformReader{
  public final Path path;
  private final RandomAccessFile raf;
  private final long end;
  protected final FileChannel channel;
  
  private final ByteBuffer header = ByteBuffer.allocateDirect(8 + 4 + 4);
  
  protected long pos = 0;
  
  public long cellId;
  protected int size;
  protected int bytes;
  
  private TransformReader(Path path) throws IOException {
    this.path = path;
    this.raf = new RandomAccessFile(path.toFile(), "r");
    this.end = raf.length();
    this.channel = raf.getChannel();
    
    readHeader();
  }
  
  protected void skip(){
    pos += bytes;
  }
  
  protected void readHeader() throws IOException{
    header.clear();
    channel.read(header,pos);
    pos += header.capacity();  
  
    header.flip();
    this.cellId = header.getLong();
    this.size = header.getInt();
    this.bytes = header.getInt();
  }
  
  public long getCellId(){
    return cellId;
  }
  
  public boolean hasNext(){
    return pos+bytes < end;
  }
  
  public void next() throws IOException{
    skip();
    readHeader();
  }
  
  @Override
  public String toString() {
    return String.format("%d:%d %d, (#%d  %d)", (pos - 16),end,cellId, size, bytes);
  }
 
  
  public static class NodeReader extends TransformReader { 
    public NodeReader(Path path) throws IOException {
      super(path);
    }
    
    public Set<OSHNode> get() throws IOException{
      final ByteBuffer data = ByteBuffer.allocateDirect(bytes);
      channel.read(data, pos);
      data.flip();
      
      Set<OSHNode> ret = new TreeSet<>((a,b) -> Long.compare(a.getId(), b.getId()));
      while(data.hasRemaining()){
        int length = data.getInt();
        byte[] content = new byte[length];
        data.get(content);
        OSHNode node = OSHNode.instance(content, 0, length);
        System.out.println(node.getId());
        ret.add(node);
      }
      ret.stream().filter(node -> node.getId() == 553542L).forEach(System.out::println);
      return ret;
    }
  }
  
  public static class NodeReaders {    
     PriorityQueue<NodeReader> queue = new PriorityQueue<>((a,b) -> {
       int c = ZGrid.ORDER_DFS_TOP_DOWN.compare(a.cellId, b.cellId);
       if(c != 0)
         return c;
       return a.path.compareTo(b.path);
     });
     
     public NodeReaders(NodeReader... readers){
       for(NodeReader r : readers){
         queue.add(r);
       }
     }
     
     public long cellId(){
       return queue.peek().cellId;
     }
     
     public Set<OSHNode> get() throws IOException{
          List<NodeReader> readers = new ArrayList<>(queue.size());
          NodeReader reader = queue.poll();
          readers.add(reader);
          System.out.println(reader.cellId+ " "+reader.path);
          Set<OSHNode> nodes = reader.get();
          nodes.stream().filter(node -> node.getId() == 553542L).forEach(System.out::println);
          while(!queue.isEmpty() && queue.peek().cellId == reader.cellId){
            nodes.addAll(queue.peek().get());
            readers.add(queue.poll());
          }
          readers.stream().filter(NodeReader::hasNext).forEach(r -> {
            try {
              r.next();
              queue.add(r);
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            
          });
          return nodes;
     }

    public boolean hasNext() {
      return !queue.isEmpty();
    }
     
  }
  
  
  public static class WayReader extends TransformReader { 
    public WayReader(Path path) throws IOException {
      super(path);
    }
    
    public Set<TransformOSHWay> get() throws IOException{
      final ByteBuffer data = ByteBuffer.allocateDirect(bytes);
      channel.read(data, pos);
      data.flip();
      
      
      Set<TransformOSHWay> ret = new TreeSet<>((a,b) -> Long.compare(a.getId(), b.getId()));
      while(data.hasRemaining()){
        int length = data.getInt();
        byte[] content = new byte[length];
        data.get(content);
        TransformOSHWay e = TransformOSHWay.instance(content, 0, length);
        ret.add( e);
        
      }
      return ret;
    }
  }
  
  
  
  
  
  
  
  
  public static void load(List<Grid<TransformOSHWay>> zoomLevel, int zoom, Long2ObjectMap<OSHNode> nodes){
     Grid<TransformOSHWay> grid = zoomLevel.get(zoom);
     List<Long> toRemove = new ArrayList<>();
     if(grid != null){
       System.out.println("remove " +zoom);
       zoomLevel.set(zoom, null);
     }
  }
 
  /*
  private static Roaring64NavigableMap getUpperLevelRefMap(List<Grid<TransformOSHWay>> zoomLevel, int zoom) {
    while(zoom > 0){
      Grid<TransformOSHWay> grid = zoomLevel.get(--zoom);
      if(grid != null)
        return grid.refMap;
    }
    return null;
  }
  */
  
  private static Set<Long> getUpperLevelRefMapSet(List<Grid<TransformOSHWay>> zoomLevel, int zoom) {
    while(zoom > 0){
      Grid<TransformOSHWay> grid = zoomLevel.get(--zoom);
      if(grid != null)
        return grid.refMapSet;
    }
    return null;
  }
  
  public static class Grid<T> {
      public final long cellId;
      public final Set<T> entities;
      //public final Roaring64NavigableMap refMap;
      public final Set<Long> refMapSet;
      
      private Grid(long cellId, Set<T> entities, Set<Long> refMapSet) {
        this.cellId = cellId;
        this.entities = entities;
        this.refMapSet = refMapSet;
      }
     
      /*
      public static <T> Grid<T> of(long cellId, Set<T> ways, Roaring64NavigableMap refMap) {
        return new Grid<T>(cellId,ways,refMap);
      }
      */
      
      public static <T> Grid<T> of(long cellId, Set<T> ways, Set<Long> refMapSet) {
        return new Grid<T>(cellId,ways,refMapSet);
      }
      
      
  }

  public static void main(String[] args) throws FileNotFoundException, IOException {

    final Path workDirectory = Paths.get(".", "temp");

    
    int maxZoom = -1;
    
    List<Grid<TransformOSHWay>> zoomLevel = new ArrayList<>(20);
  
      
    
    
     
    WayReader wayReader = new WayReader(workDirectory.resolve(String.format("transform_%s_%02d", OSMType.WAY.toString().toLowerCase(), 0)));
  
    if(wayReader.cellId == -1){
      wayReader.next();
    }
    
    Long2ObjectMap<OSHNode> nodes = new Long2ObjectAVLTreeMap<>();
    NodeReader nodeReader = new NodeReader(workDirectory.resolve(String.format("transform_%s_%02d", OSMType.NODE.toString().toLowerCase(), 0)));
    
    while(true){
      final long cellId = wayReader.cellId;
      final long zId = cellId;
      final int zoom =  ZGrid.getZoom(zId);
      
      for(int i = maxZoom; i >= zoom; i--){
        load(zoomLevel,i, nodes);
      }
      
      // init zoomLevel
      if(maxZoom < zoom){
        for(int i = maxZoom; i < zoom; i++){
          zoomLevel.add(null);
        }
        maxZoom = zoom;
      }
      
      Set<TransformOSHWay> ways = wayReader.get();
      
      
      //Roaring64NavigableMap nodeRefMap = new Roaring64NavigableMap();
      Set<Long> nodeRefMapSet = new HashSet<>();
      
      ways.stream()
      .map(osh -> osh.getNodeIds())
      .forEach(ids -> {for(long id : ids) nodeRefMapSet.add(id);});
      
      //Roaring64NavigableMap upperRefMap = getUpperLevelRefMap(zoomLevel, zoom);
      Set<Long> upperRefMapSet = getUpperLevelRefMapSet(zoomLevel,zoom);
      if(upperRefMapSet!= null)
        nodeRefMapSet.addAll(upperRefMapSet);    
      //nodeRefMap.runOptimize();
      zoomLevel.set(zoom,Grid.of(cellId, ways,nodeRefMapSet));
      
      while(nodeReader.cellId <= cellId){
        for(OSHNode node :nodeReader.get()){
          if(upperRefMapSet.contains(node.getId()))
            nodes.put(node.getId(), node);
        }
        if(!nodeReader.hasNext()){
          System.out.println("no more nodes");
          break;
        }
        nodeReader.next();
      }
        
      
      if(!wayReader.hasNext())
        break;
      wayReader.next();
    }
    
    System.out.println("fertig");
    
    if(true)
      return;
    
    wayReader.next();
    
    long zId = wayReader.cellId;
    int zoom  = ZGrid.getZoom(zId);
    long cellId = ZGrid.getIdWithoutZoom(zId);
    
  /*  
    
    Set<OSHWay> ways = wayReader.get();
    Roaring64NavigableMap nodeRefMap = new Roaring64NavigableMap();
    ways.stream()
    .flatMap(osh -> StreamSupport.stream(osh.spliterator(), false))
    .flatMap(osm -> (Stream<OSMMember>) Stream.of(osm.getRefs()))
    .mapToLong((OSMMember member) -> member.getId())
    .forEach(nodeRefMap::addLong);
    nodeRefMap.runOptimize();
    System.out.println(nodeRefMap);
   */ 
    
    if(true)
      return;
   /* 
    System.out.printf("way:%d=%d%n",zoom,cellId);
    
    List<NodeReader> reader = new ArrayList<>(2); 
    reader.add(new NodeReader(workDirectory.resolve(String.format("transform_%s_%02d", OSMType.NODE.toString().toLowerCase(), 0))));
    reader.add(new NodeReader(workDirectory.resolve(String.format("transform_%s_%02d", OSMType.NODE.toString().toLowerCase(), 1))));

   
    reader.forEach(nodeReader -> { if(nodeReader.cellId == -1)
      try {
        nodeReader.next();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }});
    
    reader.get(0).next();
    while(ZGrid.ORDER_DFS_TOP_DOWN.compare(reader.get(0).cellId,zId ) <= 0){
      System.out.printf("node:%d=%d%n",ZGrid.getZoom(reader.get(0).cellId), ZGrid.getIdWithoutZoom(reader.get(0).cellId));
   
      
      reader.get(0).next();
    }
    */
    //System.out.printf("->node:%d=%d%n",ZGrid.getZoom(reader.get(0).cellId), ZGrid.getIdWithoutZoom(reader.get(0).cellId));
    

    
    //wayReader.get().forEach((id,osh) -> System.out.println(osh));
    
    
    /*
   // System.out.println(reader);

 //   reader.get().forEach((id,node) -> {System.out.println( node);});
      reader.next();
      System.out.println(reader);
      Map<Long,OSHNode> nodes = reader.get();
      System.out.println("nodes "+nodes.size());
      System.out.println(wayReader);
      wayReader.get().forEach((id,data) -> {
        //System.out.println( data);
        data.forEach((osm) -> {
          for(OSMMember m : osm.getRefs()){
            nodes.remove(m.getId());
            
          }
        });
       });
      System.out.println("nodes "+nodes.size());
    

*/
  }

  

}
