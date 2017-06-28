package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;


public abstract class Attribute {

  private int type_id;

  private String type;

  protected volatile Map<Long,Double> values = new HashMap<Long,Double>(); //long cellenid 
  //maps in 

  public void setAttributeTypeId(int type_id){
    this.type_id = type_id;
  }
  

  public int getAttributeTypeId(){
    return type_id;
  }
  

  public String getType(){
    return type;
  }
  

  public void setType(String type) {
    this.type = type;
    
  }
 
  public double defaultValue(){
    return 0.0;
  }
  

  //TODO im Falle der TempDB kommen hier pro cell_id mehrere Werte. D.h. CellID ist nicht unique 
  protected double getValue(long cell_id){
    if(values.get(cell_id) == null){
      return defaultValue();
    }
    return values.get(cell_id);
  }
  
  //TODO CellID not unique, better other datastructure 
  protected void putValue(long cell_id, double value){
    values.put(cell_id, value);
  }
  
  
  protected boolean needArea(OSMatrixProcessor.TABLE table){
    return false;
  }
  
  public abstract String getName();
    
  public abstract String getDescription();
   
  public abstract String getTitle();
 
  public abstract List<TABLE> getDependencies();
      
  protected abstract double doUpdate(OSMatrixProcessor.TABLE table,long cell_id, double old_value,ResultSet row) throws Exception;

  protected abstract String where(OSMatrixProcessor.TABLE table);
  
  public void update(OSMatrixProcessor.TABLE table,long cell_id, ResultSet row){
    Double last_value = values.get(cell_id);
    if(last_value == null)
      last_value = defaultValue();
      

    try {
      Double new_value = doUpdate(table,cell_id, last_value, row);
      if(last_value.compareTo(new_value) != 0){
        values.put(cell_id, new_value);
      }
    } catch (Exception e) {
      
    }
    
  }

  protected void beforSend(){
    
  }
  

  public  Map<Long,Double> getValues(){
    beforSend();
    return values;
  }

  
  

}

