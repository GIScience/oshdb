package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;

public class TotalNumberOfDrinkingFountain extends Attribute {

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return "TotalNumberOfDrinkingFountain";
  }

  @Override
  public String getDescription() {
    // TODO Auto-generated method stub
    return "The total Number of Drinking Fountains in the respective Cell. We looked for amenity drinking water";
  }

  @Override
  public String getTitle() {
    // TODO Auto-generated method stub
    return "Total Number Of Drinking Fountain";
  }

  @Override
  public List<TABLE> getDependencies() {
    // TODO Auto-generated method stub
    return Arrays.asList(TABLE.NODE);
  }

  @Override
  protected double doUpdate(TABLE table, long cell_id, double old_value, ResultSet row) throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  protected String where(TABLE table) {
    // TODO Auto-generated method stub
    return null;
  }

}
