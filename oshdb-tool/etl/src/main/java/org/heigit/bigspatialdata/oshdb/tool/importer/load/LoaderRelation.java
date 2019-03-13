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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransfomRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.reader.TransformRelationReaders;


public class LoaderRelation extends Loader{
  
  public static interface Handler {
    public void handleRelationGrid(long cellId, Collection<TransfomRelation> entities,Collection<TransformOSHNode> nodes, Collection<TransformOSHWay> ways);
  }
  
  private static class Grid {
    long cellId;
    List<TransfomRelation> entities = null;
    Set<Long> nodesSet = Collections.emptySet();
    List<TransformOSHNode> nodeforGrid = null;
    
    Set<Long> waysSet = Collections.emptySet();
    List<TransformOSHWay> wayforGrid = null;
    
    public void clear() {
      entities = null;
      nodesSet = Collections.emptySet();
      nodeforGrid = null;
      waysSet = Collections.emptySet();
      wayforGrid = null;
    }
  }
  
  
  final List<Grid> zoomLevel = new ArrayList<>(20);
  int currentZoom;
  int maxZoom = -1;
  final int maxZoomLevel;
  int lastZoom = -1;
  
  
  Set<Long> nodesForCellSet = new HashSet<>();
  List<TransformOSHNode> nodesForGrid;
  Set<Long> waysForCellSet = new HashSet<>();
  List<TransformOSHWay> waysForGrid;
  
  final TransformRelationReaders reader;
  final Handler handler;
  final LoaderNode nodeLoader;
  final LoaderWay wayLoader;
  
  public LoaderRelation(Path workDirectory,Handler handler, int minEntitiesPerCell,LoaderNode nodeLoader, LoaderWay wayLoader, int maxZoomLevel) throws IOException {
    super(minEntitiesPerCell);
    Path[] files;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(workDirectory, "transform_relation_*")) {
      files = StreamSupport.stream(stream.spliterator(), false).collect(Collectors.toList()).toArray(new Path[0]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.reader = new TransformRelationReaders(files);
    this.handler = handler;
    this.nodeLoader = nodeLoader;
    nodeLoader.addLoader(this);
    this.wayLoader = wayLoader;
    wayLoader.addLoader(this);
    this.maxZoomLevel = Math.max(1, maxZoomLevel);
    
  }
  
  @Override
  public void load(long cellId2, boolean all) {
    if (!reader.hasNext())
      return;

    if(reader.getCellId() == -1){
      Set<TransfomRelation> set = reader.next();
      handler.handleRelationGrid(-1, set,Collections.emptyList(),Collections.emptyList());
    }
    
    while (reader.hasNext() && (all || ZGrid.ORDER_DFS_TOP_DOWN.compare(reader.getCellId(), cellId2) <= 0)) {
      final long cellId = reader.getCellId();
      final int zoom = ZGrid.getZoom(cellId);
      currentZoom = zoom;
      
      final Set<TransfomRelation> entities = reader.next();
      
      maxZoom = initZoomLevel(maxZoom, zoom, zoomLevel, () -> new Grid());
      
      nodesForCellSet = new HashSet<>();
      waysForCellSet = new HashSet<>();
      
      entities.forEach(e -> {
        for(Long id : e.getNodeIds())
          nodesForCellSet.add(id);
        for(Long id : e.getWayIds())
          waysForCellSet.add(id);
      });
      
      nodesForGrid = new ArrayList<>(nodesForCellSet.size());
      waysForGrid = new ArrayList<>(waysForCellSet.size());
      
      wayLoader.load(cellId, false);
      
      store(zoom);
      
      Grid grid = zoomLevel.get(zoom);
      grid.cellId = cellId;
      grid.entities = new ArrayList<>(entities);
      grid.nodeforGrid = nodesForGrid;
      grid.nodesSet =nodesForCellSet;
      grid.wayforGrid = waysForGrid;
      grid.waysSet = waysForCellSet;

      lastZoom = zoom;
    }
    
    if (!reader.hasNext()) {
      wayLoader.load(0, true);
      store(0);
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
            parent.nodeforGrid = grid.nodeforGrid;
            parent.nodesSet = grid.nodesSet;
            parent.wayforGrid = grid.wayforGrid;
            parent.waysSet = grid.waysSet;
          }else{
            parent.entities.addAll(grid.entities);
            
            parent.nodeforGrid.addAll(grid.nodeforGrid);
            for(TransformOSHNode osh : grid.nodeforGrid){
              parent.nodesSet.remove(osh.getId());
            }
            parent.nodesSet.addAll(grid.nodesSet);
            
            parent.wayforGrid.addAll(grid.wayforGrid);
            for(TransformOSHWay osh: grid.wayforGrid){
              parent.waysSet.remove(osh.getId());
            }
          }
          
          grid.clear();
          continue;
      }
      
    //some nodes could still be left, maybe they are invalid!
      //TODO or should we ignore them?
      grid.nodesSet.forEach(id -> {
        TransformOSHNode e = nodeLoader.invalidNodes.get(id.longValue());
        if(e != null)//TODO relations missing nodes!
          grid.nodeforGrid.add(e);
      });

      grid.waysSet.forEach(id -> {
        TransformOSHWay e = wayLoader.invalids.get(id.longValue());
        if(e != null) //TODO Relations missing ways!
          grid.wayforGrid.add(e);
      });

      handler.handleRelationGrid(grid.cellId, grid.entities, grid.nodeforGrid, grid.wayforGrid);
      grid.clear();
    }
  }

  @Override
  public void visitNode(TransformOSHNode osh) {
    final long id = osh.getId();
    final Long id2 = osh.getId();
    if(nodesForCellSet.contains(id2)){
      nodesForGrid.add(osh);
      nodesForCellSet.remove(id2);
    }else {
      for(int i=lastZoom; i>=0; i--){
        final Grid g = zoomLevel.get(i);
        if(g.nodesSet.contains(id2)){
          g.nodeforGrid.add(osh);
          g.nodesSet.remove(id2);;
          break;
        }
      }
    }  
  }
  
  @Override
  public void visitWay(TransformOSHWay osh) {
    final long id = osh.getId();
    final Long id2 = osh.getId();
    if(waysForCellSet.contains(id2)){
      waysForGrid.add(osh);
      waysForCellSet.remove(id2);
    }else {
      for(int i=lastZoom; i>=0; i--){
        final Grid g = zoomLevel.get(i);
        if(g.waysSet.contains(id2)){
          g.wayforGrid.add(osh);
          g.waysSet.remove(id2);;
          break;
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    Path workDirectory = Paths.get("./temp/nepal");
    Path[] files;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(workDirectory, "transform_relation_*")) {
      files = StreamSupport.stream(stream.spliterator(), false).collect(Collectors.toList()).toArray(new Path[0]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    TransformRelationReaders reader = new TransformRelationReaders(files);
    
    while(reader.hasNext()){
      long cellId = reader.getCellId();
      Optional<TransfomRelation> opt = reader.next().stream().filter(r -> r.getId() == 3798196L).findAny();
      if(opt.isPresent()){
        System.out.printf("%d:%d (%d) %s%n",ZGrid.getZoom(cellId),ZGrid.getIdWithoutZoom(cellId),cellId,opt.get());
        break;
      }
      
      
    }
    
    
  }

}
