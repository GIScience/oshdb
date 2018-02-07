package org.heigit.bigspatialdata.oshdb.tool.importer.load;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;


public abstract class Loader {

  protected final int minEntitiesPerCell;

  protected List<Loader> loaders = new ArrayList<>(2);

  
  public Loader(int minEntitiesPerCell){
    this.minEntitiesPerCell = minEntitiesPerCell;
  }
  
  public void load(){
    load(Long.MAX_VALUE,true);
  }
  public void load(long cellId){
    load(cellId,false);
  }
  
  public abstract void load(long cellId2, boolean all);

  public void addLoader(Loader loader) {
    this.loaders.add(loader);
  }

  public void visitNode(TransformOSHNode node) {

  }

  public void visitWay(TransformOSHWay way) {

  }

  public <T> int initZoomLevel(int maxZoom, int zoom, List<T> zoomLevel, Supplier<T> add) {
    if (maxZoom < zoom) {
      for (int i = maxZoom; i < zoom; i++) {
        zoomLevel.add(add.get());
      }
      maxZoom = zoom;
    }
    return maxZoom;
  }

}
