package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class TotalNumberOfTags extends Attribute{
//TODO NOT IMPLEMENTED YET !!!!!!!!!!!!!!!!
  @Override
  public String getName() {
     return "TotalNumberOfTags";
  }

  @Override
  public String getDescription() {
      return "Total number of Tags";
  }

  @Override
  public String getTitle() {
       return "TotaNumberOfTags";
  }

  @Override
  public List<TABLE> getDependencies() {
       return Arrays.asList(TABLE.NODE, TABLE.WAY,TABLE.RELATION);
  }


  @Override
  public AttributeCells compute(SimpleFeatureSource cellsIndex, OSHEntity<OSMEntity> osh, TagLookup tagLookup,
      List<Long> timestampsList, int attributeId) {
    
    AttributeCells oshresult = new AttributeCells();
    for ( Map.Entry<Long,OSMEntity> entry : osh.getByTimestamps(timestampsList).entrySet() ){
      
      long ts = entry.getKey();
      OSMEntity osm = entry.getValue();
      
      Geometry osmGeometry;
      try{
      osmGeometry= osm.getGeometry(ts, tagLookup.getTagInterpreter());
      } catch (Exception e) {
        osmGeometry = null;
      }
      if(osmGeometry == null)
        continue;
      Point osmCentroid = osmGeometry.getCentroid();  
      
      
    }

    return oshresult;
  }

  @Override
  public void aggregate(AttributeCells gridcellOutput, AttributeCells oshresult, List<Long> timestampsList) {
    // TODO Auto-generated method stub
    
  }

}
