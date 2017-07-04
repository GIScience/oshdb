package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Map;


public class AttributeCells {
     
  private Map<Integer,CellTimeStamps> attrCells;
  
  CellTimeStamps get(int attributeId){
    CellTimeStamps ret = attrCells.get(attributeId);
    if(ret == null){
      ret = new CellTimeStamps();
      attrCells.put(attributeId, ret);
    }
    
    return ret;
      
  }
}
