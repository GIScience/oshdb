package org.heigit.bigspatialdata.oshdb.tool.importer.load;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.reader.TransformWayReaders;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

public class LoaderWay extends Loader {

  public static interface Handler {
    public void handleWayGrid(long cellId, Collection<TransformOSHWay> ways, Collection<TransformOSHNode> nodes);
  }
  
  private static class Grid {
    long cellId = -2;
    List<TransformOSHWay> entities = null;
    Set<Long> nodesSet = Collections.emptySet();
    List<TransformOSHNode> forGrid = null;

    public void clear() {
      entities = null;
      forGrid = null;
      nodesSet = Collections.emptySet();
    }
  }

  final List<Grid> zoomLevel = new ArrayList<>(20);
  int currentZoom;
  int maxZoom = -1;
  final int maxZoomLevel;
  int lastZoom = -1;

  final TransformWayReaders reader;
  final Handler handler;

  final LoaderNode nodeLoader;

  public final Long2ObjectMap<TransformOSHWay> invalids = new Long2ObjectAVLTreeMap<>();
  
  Set<Long> nodesForCellSet = new HashSet<>();
  List<TransformOSHNode> forGrid;

  public LoaderWay(Path workDirectory, Handler handler, int minEntitiesPerCell, LoaderNode nodeLoader,int maxZoomLevel) throws IOException {
    super(minEntitiesPerCell);
    Path[] files;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(workDirectory, "transform_way_*")) {
      files = StreamSupport.stream(stream.spliterator(), false).collect(Collectors.toList()).toArray(new Path[0]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.reader = new TransformWayReaders(files);
    this.handler = handler;
    this.nodeLoader = nodeLoader;
    nodeLoader.addLoader(this);
    this.maxZoomLevel = Math.max(1, maxZoomLevel);
  }

  public int count = 0;
  
  public void load(long cellId2, boolean all) {
    if (!reader.hasNext())
      return;

    if(reader.getCellId() == -1){
      Set<TransformOSHWay> set = reader.next();
      set.stream().forEach(osh -> invalids.put(osh.getId(), osh));
      handler.handleWayGrid(-1, invalids.values(),Collections.emptyList());
    }
    
    while (reader.hasNext() && (all || ZGrid.ORDER_DFS_TOP_DOWN.compare(reader.getCellId(), cellId2) <= 0)) {
      final long cellId = reader.getCellId();
      final int zoom = ZGrid.getZoom(cellId);
      currentZoom = zoom;
      
      final Set<TransformOSHWay> ways = reader.next();
      count++;
      ways.forEach(way -> {
        for (Loader loader : loaders)
          loader.visitWay(way);
      });

      maxZoom = initZoomLevel(maxZoom, zoom, zoomLevel, () -> new Grid());

      nodesForCellSet = new HashSet<>();
      ways.stream().forEach(osh -> {
        for(Long id : osh.getNodeIds())
          nodesForCellSet.add(id);
       });
      
      forGrid = new ArrayList<>(nodesForCellSet.size());
      
      nodeLoader.load(cellId,false);

      store(zoom);

      Grid grid = zoomLevel.get(zoom);
      grid.cellId = cellId;
      grid.entities = new ArrayList<>(ways);
      grid.forGrid = forGrid;
      grid.nodesSet = nodesForCellSet;

      lastZoom = zoom;
    }

    if (!reader.hasNext()) {
      nodeLoader.load(0, true);
      store(0);
      nodesForCellSet = null;
    }
  }

  private void store(int zoom) {
    for (int i = lastZoom; i >= zoom; i--) {
      Grid grid = zoomLevel.get(i);
      if (grid == null || grid.entities == null)
        continue;

      if (i > maxZoomLevel || (grid.entities.size() < minEntitiesPerCell && i > 0)) {
        Grid parent = zoomLevel.get(i-1);
       
          if (parent.entities == null) {
            parent.cellId = ZGrid.getParent(grid.cellId);
            parent.entities = grid.entities;
            parent.forGrid = grid.forGrid;
            parent.nodesSet = grid.nodesSet;
          }else{
            parent.entities.addAll(grid.entities);
            parent.forGrid.addAll(grid.forGrid);
            
            for(TransformOSHNode osh : grid.forGrid){
              parent.nodesSet.remove(osh.getId());
            }
            parent.nodesSet.addAll(grid.nodesSet);
          }            
          grid.clear();
          continue;
        }
        
      
      for(TransformOSHNode osh : grid.forGrid){
        Long id = osh.getId();
        for(int j= i-1; j>=0; j--){
          final Grid parent = zoomLevel.get(j);
          if(parent.nodesSet.contains(id)){
            parent.forGrid.add(osh);
            parent.nodesSet.remove(id);
            break;
          }
        }
      }
      
      //some nodes could still be left, maybe they are invalid!
      //TODO or should we ignore them?
      grid.nodesSet.forEach(id -> {
        TransformOSHNode node = nodeLoader.invalidNodes.get(id.longValue());
        if(node != null)
        	grid.forGrid.add(node);
      });
      
      handler.handleWayGrid(grid.cellId, grid.entities, grid.forGrid);
      grid.clear();
    }
  }



  @Override
  public void visitNode(TransformOSHNode node) {
    final long id = node.getId();
    final Long id2 = node.getId();
    if(nodesForCellSet.contains(id2)){
      forGrid.add(node);
      nodesForCellSet.remove(id2);
    }else {
      for(int i=lastZoom; i>=0; i--){
        final Grid g = zoomLevel.get(i);
        if(g.nodesSet.contains(id2)){
          g.forGrid.add(node);
          g.nodesSet.remove(id2);;
          break;
        }
      }
    }    
  }
  
  public static void main(String[] args) throws IOException {
    Path workDirectory = Paths.get("./temp");
    TransformWayReaders reader = new TransformWayReaders(
        workDirectory.resolve(String.format("transform_%s_%02d", OSMType.WAY.toString().toLowerCase(), 0)));
    
    int count=0;
    int sum = 0;
    while(reader.hasNext()){
      sum += reader.next().size();
      count++;
    }
    
    System.out.println("count "+count+" sum "+sum);
  }
  
}
