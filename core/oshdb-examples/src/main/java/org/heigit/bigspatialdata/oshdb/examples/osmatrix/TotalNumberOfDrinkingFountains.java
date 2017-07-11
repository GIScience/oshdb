package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import com.vividsolutions.jts.geom.Point;

public class TotalNumberOfDrinkingFountains extends Attribute {

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
  public AttributeCells compute(SimpleFeatureSource cellsIndex, OSHEntity<OSMEntity> osh, TagLookup tagLookup, List<Long> timestampsList, int attributeId) {
    
    AttributeCells oshresult = new AttributeCells();
    
    
    Pair tagKeyValue = tagLookup.getAllKeyValues().get("amenity").get("drinking_water");
    int tagKey = (int) tagKeyValue.getLeft();
    int tagValue = (int) tagKeyValue.getRight();
    
    //filter all osh that have never been an amenity
    if ( !osh.hasTagKey(tagKey) || !( osh.getType()== OSHEntity.NODE)) return oshresult; //empty
    
    //osh has at some point in time a tag amenity
    //System.out.println("Here");
    //System.out.println(tagKey);
    //System.out.println(cellsIndex.getSchema().getGeometryDescriptor().getLocalName());
    
    
    for ( Map.Entry<Long,OSMEntity> entry : osh.getByTimestamps(timestampsList).entrySet() ){
      
      long ts = entry.getKey();
      OSMNode osm = (OSMNode) entry.getValue();
      
      if(! osm.hasTagValue(tagKey, tagValue) || !osm.isVisible() ) return oshresult;
      
//      System.out.printf("%d  ->  %s\n",ts,osm);
      Point osmPoint = osm.getGeometry();

      
      FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
      Filter filter = ff.intersects(ff.property(cellsIndex.getSchema().getGeometryDescriptor().getLocalName()),
          ff.literal(osmPoint));
      
      try {
        FeatureCollection<SimpleFeatureType, SimpleFeature> osmatrixCells = cellsIndex.getFeatures( filter) ;
      
        FeatureIterator<SimpleFeature> features = osmatrixCells.features();
        
        while (features.hasNext()) {
          
          SimpleFeature feature = features.next();
          
          Double cellId_ = Double.parseDouble( feature.getAttribute("id").toString() );
          long cellId = cellId_.longValue();
//          System.out.println("attributeId " + attributeId);
         oshresult.get(attributeId).get(cellId).get(ts).setValueWeight(1, 1);
         
          
        }
        
        //if (!osmatrixCells.isEmpty()) System.out.println(osmatrixCells.features().next().getAttributes());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      
      
     
      
    }
   
    // Iterator<OSMEntity> osmIterator
    return oshresult;
  }

  @Override
  public void aggregate(AttributeCells gridcellOutput, AttributeCells oshresult, List<Long> timestampsList) {
    
   for (Map.Entry<Integer, CellTimeStamps>  attributeCell : oshresult.map.entrySet()){
     
     final CellTimeStamps cellTimestamps = attributeCell.getValue();
     
     for ( Map.Entry<Long, TimeStampValuesWeights> cellTimestamp : cellTimestamps.map.entrySet()){
       
       final TimeStampValuesWeights timestampValueWeights = cellTimestamp.getValue();
       
       for (Long long1 : timestampsList) {
         timestampValueWeights.map.putIfAbsent(long1, new ValueWeight());
       } 
       
       for ( Map.Entry<Long, ValueWeight> timestampValueWeight : timestampValueWeights.map.entrySet() ){
         
         final ValueWeight valueWeight = timestampValueWeight.getValue();
         //System.out.println("valueWeight: " + valueWeight.getValue());
         
         ValueWeight partial = gridcellOutput.get(attributeCell.getKey()).get(cellTimestamp.getKey()).get(timestampValueWeight.getKey());
//         System.out.println("partial before: " + partial.getValue());
         //count
         partial.setValue(partial.getValue() + valueWeight.getValue() );
//         System.out.println("partial after: " + partial.getValue());
         
         gridcellOutput
         .get(attributeCell.getKey())
         .get(cellTimestamp.getKey())
         .get(timestampValueWeight.getKey())
         .setValue(partial.getValue());
         
       }
       
     }
     
   }  
   
    
  }

}
