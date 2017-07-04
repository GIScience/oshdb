package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class AttributeCells {
     
  public AttributeCells() {
    //super();
    this.attrCells = new HashMap<Integer, CellTimeStamps>();
  }

  private Map<Integer,CellTimeStamps> attrCells;
  
  CellTimeStamps get(int attributeId){
    
    if (attrCells.containsKey(attributeId)){
      return attrCells.get(attributeId);
    }
    else
    {
      attrCells.put(attributeId, attrCells.get(attributeId));
      return attrCells.get(attributeId); 
    }
      
  }
}
