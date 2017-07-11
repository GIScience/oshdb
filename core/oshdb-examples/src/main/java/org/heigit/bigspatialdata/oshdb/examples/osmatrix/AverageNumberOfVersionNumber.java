package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class AverageNumberOfVersionNumber extends Attribute {

private static final Logger logger = Logger.getLogger(AverageNumberOfAttributes.class);
	
	
	Map<Long, Double> cellVersionNumb = new HashMap<Long, Double>();
	
	Map<Long, Long> cellFeatureNumb = new HashMap<Long,Long>();

	@Override
	public String getName() {
		return "AverageNumberOfVersionNumber";
	}

	@Override
	public String getDescription() {
		return "The average version number of all objects within the given cell.";
	}
	
	@Override
	public double defaultValue() {
		return 1.0;
	}

	@Override
	public List<TABLE> getDependencies() {
		return Arrays.asList(TABLE.WAY,TABLE.WAY,TABLE.RELATION);
	}

	@Override
	protected void beforSend() {

	}
	
	@Override
	protected boolean needArea(TABLE table) {
		return false;
	}
	
	@Override
	public String getTitle() {
		return "Average version number";
	}

  @Override
  public AttributeCells compute(SimpleFeatureSource cellsIndex, OSHEntity<OSMEntity> osh, TagLookup tagLookup,
      List<Long> timestampsList, int attributeId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void aggregate(AttributeCells gridcellOutput, AttributeCells oshresult, List<Long> timestampList) {
    // TODO Auto-generated method stub
    
  }


}
