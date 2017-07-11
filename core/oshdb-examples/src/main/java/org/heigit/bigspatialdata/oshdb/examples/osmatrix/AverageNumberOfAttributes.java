package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.examples.osmatrix.OSMatrixProcessor.TABLE;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;


public class AverageNumberOfAttributes extends Attribute {

	private static final Logger logger = Logger
			.getLogger(AverageNumberOfAttributes.class);

	Map<Long, Long> cellFeature = new HashMap<Long, Long>();
	
	Map<Long, Long> cellNumAttributes = new HashMap<Long, Long>();

	@Override
	public String getName() {
		return "AverageNumbAttr";
	}

	@Override
	public String getDescription() {
		return "The average number of attributes attached to any object within the given cell.";
	}

	@Override
	public List<TABLE> getDependencies() {
		return Arrays.asList(TABLE.NODE, TABLE.WAY, TABLE.RELATION);
	}


	@Override
	protected void beforSend() {

		Set<Long> cells = cellFeature.keySet();
		Long numFeat, numAttr;
		for (Long cell : cells) {

			numFeat = cellFeature.get(cell);
			if (numFeat == null) {
				numFeat = new Long(0);
			}

			numAttr = cellNumAttributes.get(cell);
			if (numAttr == null || numAttr == 0) {
				numAttr = new Long(-1);
			}

			values.put(cell, numFeat.doubleValue()/numAttr.doubleValue());
		}

	}

	@Override
	protected boolean needArea(TABLE table) {
		return false;
	}

	@Override
	public String getTitle() {
		return "Average number of attributes";
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