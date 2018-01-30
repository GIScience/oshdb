package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagId;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.SortedLong2LongMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public abstract class Transformer {
  private static class OSHDataContainer {
    private long sizeInBytesOfData = 0;
    private long estimatedMemoryUsage = SizeEstimator.objOverhead() + 2 * SizeEstimator.intField()+ SizeEstimator.linkedList();
    
    
    private long lastId = 0;
    private List<byte[]> list = new LinkedList<>();

    public OSHDataContainer add(byte[] data) {
      sizeInBytesOfData += data.length + 4; // count of bytes + 4 bytes for the length of this array
      estimatedMemoryUsage += SizeEstimator.estimatedSizeOf(data) + SizeEstimator.linkedListEntry();
      list.add(data);
      return this;
    }
  }

  private static final int PAGE_POWER = 17; // ~1MB per page

  private final TagToIdMapper tagToIdMapper;
  private final RoleToIdMapper roleToIdMapper;
  private final Long2ObjectAVLTreeMap<OSHDataContainer> collector;
  private final SortedLong2LongMap.Sink idToCellSink;
  private final SortedLong2LongMap idToCell;

  private final Map<OSMType, Long2ObjectMap<Roaring64NavigableMap>> typeRefsMaps = new HashMap<>(
      OSMType.values().length);
  private long estimatedMemoryUsage;
  private final long maxMemoryUsage;

  protected final Path workDirectory ;
  private final int workerId;
  private int fileNumber = 0;

  private final ZGrid grid;

  public Transformer(long maxMemoryUsage,int maxZoom,Path workDirectory, TagToIdMapper tagToIdMapper, int workerId) throws IOException {
    this(maxMemoryUsage,maxZoom, workDirectory, tagToIdMapper, null,workerId);
  }
  
  public Transformer(long maxMemoryUsage,int maxZoom, Path workDirectory, TagToIdMapper tagToIdMapper, RoleToIdMapper roleToIdMapper, int workerId) throws IOException {
    this.maxMemoryUsage = maxMemoryUsage;
    this.workDirectory = workDirectory;
    this.tagToIdMapper = tagToIdMapper;
    this.roleToIdMapper = roleToIdMapper;
    this.workerId = workerId;
    this.collector = new Long2ObjectAVLTreeMap<>(ZGrid.ORDER_DFS_TOP_DOWN);
    this.grid = new ZGrid(maxZoom);

    this.idToCellSink = new SortedLong2LongMap.Sink(workDirectory.resolve("transform_idToCell_" + type().toString().toLowerCase()), PAGE_POWER);
    this.idToCell = null; //new IdToCellMapping(workDirectory.resolve("idToCell_" + type().toString().toLowerCase()), 100 * 1024 * 1024);

  }

  public void transform(List<Entity> versions) {
    final Entity e = versions.get(0);
    if (type() == e.getType())
      transform(e.getId(), versions);
  }

  public void error(Throwable t) {
    System.err.println(t);
  }

  public void complete() {
    System.out.println("COMPLETE");
    saveToDisk();
    idToCellSink.close();
  }

  public int modifiedVersion(Entity entity) {
    return entity.getVersion() * (entity.isVisible() ? 1 : -1);
  }

  public int[] getKeyValue(TagText[] tags) {
    if (tags.length == 0)
      return new int[0];

    final List<TagId> ids = new ArrayList<>(tags.length);

    for (TagText tag : tags) {
      final int key = tagToIdMapper.getKey(tag.key);
      final int value = tagToIdMapper.getValue(key, tag.value);
      ids.add(TagId.of(key, value));
    }

    ids.sort((a, b) -> {
      final int c = Integer.compare(a.key, b.key);
      return (c != 0) ? c : Integer.compare(a.value, b.value);
    });
    final int[] ret = new int[tags.length * 2];
    int i = 0;
    for (TagId tag : ids) {
      ret[i++] = tag.key;
      ret[i++] = tag.value;
    }

    return ret;
  }
  
  public int getRole(String role){
    return (roleToIdMapper != null)?roleToIdMapper.getRole(role):0;
  }

  protected long getCell(long longitude, long latitude) {
    return grid.getIdSingleZIdWithZoom(longitude, latitude);
  }

  protected OSHDBBoundingBox getCellBounce(long cellId) {
    return ZGrid.getBoundingBox(cellId);
  }

  protected static long findBestFittingCellId(Set<Long> cellIds) {
    if (cellIds.isEmpty())
      return -1;

    if (cellIds.size() == 1)
      return cellIds.iterator().next();

    int minZoom = Integer.MAX_VALUE;
    for (Long cellId : cellIds) {
      minZoom = Math.min(minZoom, ZGrid.getZoom(cellId));
    }
    final int zoom = minZoom;
    // bring all to the same zoom level
    Set<Long> bestCellId = cellIds.stream().filter(id -> id >= 0).map(id -> ZGrid.getParent(id, zoom))
        .collect(Collectors.toSet());

    while (bestCellId.size() > 1) {
      cellIds = bestCellId;
      bestCellId = cellIds.stream().map(id -> ZGrid.getParent(id)).collect(Collectors.toSet());
    }
    final long cellId = bestCellId.iterator().next();
    final int cellIdZoom = ZGrid.getZoom(cellId);
    return cellId;
  }

  protected void addIdToCell(long id, long cellId) throws IOException {
    if (idToCell == null) {
      idToCellSink.put(id, cellId);
    } else {
      final long cell = idToCell.get(id);
      if (cell != cellId)
        System.err.println("id" + id + " did not match" + cellId + " stored " + cell);
    }
  }

  private void saveToDisk() {
    if (collector.isEmpty())
      return;
    final Path filePath = workDirectory
        .resolve(String.format("transform_%s_%02d_%02d", type().toString().toLowerCase(),workerId, fileNumber));
    System.out.print("transformer saveToDisk " + filePath.toString()+" ... ");
    long bytesWritten = 0;
    try (RandomAccessFile out = new RandomAccessFile(filePath.toFile(), "rw"); 
        FileChannel channel = out.getChannel()) {
      final ByteBuffer header = ByteBuffer.allocateDirect(8 + 4 + 4);
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(0);

      ObjectIterator<Entry<OSHDataContainer>> iter = collector.long2ObjectEntrySet().iterator();
      long counter = 0;
      long lastCellId = -1;
      while (iter.hasNext()) {
        Entry<OSHDataContainer> entry = iter.next();

        final long cellId = entry.getLongKey();
        final OSHDataContainer container = entry.getValue();
        final int rawSize = (int) container.sizeInBytesOfData;

        if (byteBuffer.capacity() < rawSize)
          byteBuffer = ByteBuffer.allocate(rawSize);
        else
          byteBuffer.clear();

        for (byte[] data : container.list) {
          byteBuffer.putInt(data.length);
          byteBuffer.put(data);
        }
        byteBuffer.flip();

        header.clear();
        header.putLong(cellId);
        header.putInt(container.list.size());
        if(rawSize < 0)
          System.err.println("saveToDisk rawSize negative "+cellId+" "+container.list.size());
        header.putInt(rawSize);
        header.flip();

        bytesWritten += header.remaining();
        channel.write(header);
        bytesWritten += byteBuffer.remaining();
        channel.write(byteBuffer);
        
       
        counter++;
        lastCellId = cellId;
      }
      System.out.println("done! "+bytesWritten+" bytes");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    fileNumber++;
    collector.clear();
    typeRefsMaps.clear();
    estimatedMemoryUsage = 0L;
  }

  protected void store(long cellId,long id,LongFunction<byte[]> data) {
    if (estimatedMemoryUsage > maxMemoryUsage)
      saveToDisk();

    OSHDataContainer dataContainer = collector.get(cellId);
    if (dataContainer == null) {
      dataContainer = new OSHDataContainer();
      collector.put(cellId, dataContainer);
      estimatedMemoryUsage += SizeEstimator.avlTreeEntry();
    }
    estimatedMemoryUsage -= dataContainer.estimatedMemoryUsage;
    
    dataContainer.add(data.apply(dataContainer.lastId));
    dataContainer.lastId = id;
    estimatedMemoryUsage += dataContainer.estimatedMemoryUsage;
  }

  protected void store(long cellId, long id, LongFunction<byte[]> data, LongSet nodes) {
    store(cellId,id, data);

  //  Long2ObjectMap<Roaring64NavigableMap>map=typeRefsMaps.computeIfAbsent(OSMType.NODE,k->new Long2ObjectAVLTreeMap<>(ZGrid.ORDER_DFS_TOP_DOWN))));
  //  Roaring64NavigableMap bitmap=map.computeIfAbsent(cellId,k->new Roaring64NavigableMap());estimatedMemoryUsage-=bitmap.getLongSizeInBytes();
  //  nodes.forEach(bitmap::add);bitmap.runOptimize();
  //  estimatedMemoryUsage+=bitmap.getLongSizeInBytes();
  }

  protected void store(long cellId,long id, LongFunction<byte[]> data, LongSet nodes, LongSet ways) {
    store(cellId,id, data,nodes);

//    Long2ObjectMap<Roaring64NavigableMap>map=typeRefsMaps.computeIfAbsent(OSMType.WAY,k->new Long2ObjectAVLTreeMap<>(ZGrid.ORDER_DFS_TOP_DOWN))));Roaring64NavigableMap bitmap=map.computeIfAbsent(cellId,k->new Roaring64NavigableMap());estimatedMemoryUsage-=bitmap.getLongSizeInBytes();ways.forEach(bitmap::add);bitmap.runOptimize();estimatedMemoryUsage+=bitmap.getLongSizeInBytes();

  }

  protected void store(long cellId,long id, LongFunction<byte[]> data, LongSet nodes, LongSet ways, LongSet relation) {
    store(cellId,id, data,nodes,ways);

 //   Long2ObjectMap<Roaring64NavigableMap>map=typeRefsMaps.computeIfAbsent(OSMType.RELATION,k->new Long2ObjectAVLTreeMap<>(ZGrid.ORDER_DFS_TOP_DOWN))));Roaring64NavigableMap bitmap=map.computeIfAbsent(cellId,k->new Roaring64NavigableMap());estimatedMemoryUsage-=bitmap.getLongSizeInBytes();relation.forEach(bitmap::add);bitmap.runOptimize();estimatedMemoryUsage+=bitmap.getLongSizeInBytes();

  }

  public abstract void transform(long id, List<Entity> versions);

  public abstract OSMType type();
}
