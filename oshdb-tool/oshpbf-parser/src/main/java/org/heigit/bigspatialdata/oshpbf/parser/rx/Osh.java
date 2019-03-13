package org.heigit.bigspatialdata.oshpbf.parser.rx;

import java.util.List;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;

public class Osh {
  final boolean isComplete;
  final List<Entity> versions;
  final long pos[];
  
  public Osh(boolean isComplete, List<Entity> versions, long pos) {
    this(isComplete,versions,new long[]{pos});
  }
  public Osh(boolean isComplete, List<Entity> versions, long pos1, long pos2) {
    this(isComplete,versions,new long[]{pos1,pos2});
  }
  public Osh(boolean isComplete, List<Entity> versions, long[] pos) {
    this.isComplete = isComplete;
    this.versions = versions;
    this.pos = pos;
  }
  
  public long getId(){
    return versions.get(0).getId();
  }
  
  public OSMType getType(){
    return versions.get(0).getType();
  }
  
  public List<Entity> getVersions(){
    return versions;
  }
  
  /**
   * position in bytes of the blob containing this data in the pbf
   * @return
   */
  public long[] getPos(){
    return pos;
  }
  
  @Override
  public String toString() {
    return String.format("%s[%d] complete:%s, #%d v:%d-%d", getType(), getId(), isComplete, versions.size(), versions.get(0).getVersion(), versions.get(versions.size()-1).getVersion());
  }

}
