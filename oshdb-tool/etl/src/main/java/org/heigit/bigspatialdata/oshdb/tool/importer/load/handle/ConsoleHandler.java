package org.heigit.bigspatialdata.oshdb.tool.importer.load.handle;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransfomRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;

public class ConsoleHandler extends LoaderHandler{

  
  @Override
  public void loadKeyValues(int id, String key, List<String> values) {
    final int valueLimit = 10;
    System.out.printf("%4d : k=%s,  (%d:%s%s)%n",id,key,values.size(), values.stream().limit(valueLimit).collect(Collectors.toList()).toString(),(values.size()> valueLimit)?"...":"");
  }

  @Override
  public void loadRole(int id, String role) {
    System.out.printf("%4d : r=%s%n",id,role);
  }
  
  @Override
  public void handleNodeGrid(long cellId, Collection<TransformOSHNode> nodes) {
    final int zoom = ZGrid.getZoom(cellId);
    final long id = ZGrid.getIdWithoutZoom(cellId);
    System.out.printf("load node grid (%d:%d %d) nodes:%d%n", zoom, id, cellId, nodes.size());
    
    
    
  }

  @Override
  public void handleWayGrid(long cellId, Collection<TransformOSHWay> ways, Collection<TransformOSHNode> nodes) {
    final int zoom = ZGrid.getZoom(cellId);
    final long id = ZGrid.getIdWithoutZoom(cellId);
    System.out.printf("load way grid (%d:%d %d) ways:%d nodes:%d%n", zoom, id, cellId, ways.size(), nodes.size());
    
    
    
  }

  @Override
  public void handleRelationGrid(long cellId, Collection<TransfomRelation> entities, Collection<TransformOSHNode> nodes,
      Collection<TransformOSHWay> ways) {
    final int zoom = ZGrid.getZoom(cellId);
    final long id = ZGrid.getIdWithoutZoom(cellId);
    System.out.printf("load relation grid (%d:%d %d) ways:%d nodes:%d ways:%d%n", zoom, id, cellId, entities.size(),
        nodes.size(), ways.size());

  }

}
