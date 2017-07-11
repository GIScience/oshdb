package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class LengthHighwaysMajor extends Attribute{

	private static final Logger logger = Logger.getLogger(LanduseIndustrial.class);
	
	Set<String> values = new HashSet<String>();
        
        private List<String> validTagValues = Arrays.asList("motorway", "motorway_link", 
            "primary","primary_link","secondary", "secondary_link","tertiary","tertiary_link","trunk", "trunk_link");

	@Override
	public String getName() {
		return "length_highway_major";
	}

	@Override
	public String getDescription() {
		return "The length of major highways given in meters.";
	}

	@Override
	public List<TABLE> getDependencies() {
		return Arrays.asList(TABLE.WAY);
	}

	@Override
	protected boolean needArea(TABLE table) {
		return false;
	}

	@Override
	public String getTitle() {
		return "Length of major highways";
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