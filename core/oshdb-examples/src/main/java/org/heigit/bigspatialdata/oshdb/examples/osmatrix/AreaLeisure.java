package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class AreaLeisure extends Attribute{


	private static final Logger logger = Logger.getLogger(LanduseIndustrial.class);

	Set<String> values = new HashSet<String>();

	@Override
	public String getName() {

		return "area_leisure";
	}

	@Override
	public String getDescription() {
		return "The area covered by places for leisure activities given in square meters.";
	}


	@Override
	protected boolean needArea(TABLE table) {
		return true;
	}

	@Override
	public String getTitle() {
		return "Area of leisure";
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

  @Override
  public List<TABLE> getDependencies() {
    // TODO Auto-generated method stub
    return null;
  }

}