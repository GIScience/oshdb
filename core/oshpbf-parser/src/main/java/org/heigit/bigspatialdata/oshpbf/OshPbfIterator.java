package org.heigit.bigspatialdata.oshpbf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;

public class OshPbfIterator implements Iterator<List<OSMPbfEntity>> {


  private final Iterator<OSMPbfEntity> source;
  private List<OSMPbfEntity> versions = new ArrayList<>();
  private OSMPbfEntity last = null;

  private final boolean removeDuplicatedVersions;
  
  public OshPbfIterator(Iterator<OSMPbfEntity> source){
	  this(source,true);
  }
  
  public OshPbfIterator(Iterator<OSMPbfEntity> source, boolean removeDuplicatedVersions) {
    this.source = source;
    this.removeDuplicatedVersions = removeDuplicatedVersions;
  }

  @Override
  public boolean hasNext() {
    return source.hasNext();
  }

  @Override
  public List<OSMPbfEntity> next() {
    List<OSMPbfEntity> ret = null;
    while (source.hasNext()) {
      OSMPbfEntity entity = source.next();
      if (last != null && (last.getId() != entity.getId() || last.getType() != entity.getType())) {
        ret = versions;
        versions = new ArrayList<>();
      }
      
      //TODO remove duplicated Versions, should we do this in the OsmIterator instead?
      if(removeDuplicatedVersions && last != null){
    	  if(last.getVersion() != entity.getVersion())
    		  versions.add(entity);
    	  // else skip
      }else{      
    	  versions.add(entity);
      }
      last = entity;
      if(ret != null)
        return ret;
    }
    return versions;
  }

}
