package org.heigit.bigspatialdata.hosmdb.grid;

import java.io.Serializable;

public class HOSMCell implements Serializable{
  private static final long serialVersionUID = 1L;
  protected final long id;
  protected final int level;
  
  protected final long baseTimestamp;
  
  protected final long baseLongitude;
  protected final long baseLatitude;
  
  protected final long baseId;
  
  protected final int[] index;
  protected final byte[] data;
  
  public HOSMCell(final long id, 
      final int level,
      final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,final int[] index, final byte[] data){
    
    this.id = id;
    this.level = level;
    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;
    this.baseId = baseId;
    
    this.index = index;
    this.data = data;
  }
  
  public long getId() {
    return id;
  }

  public int getLevel() {
    return level;
  }
}
