package org.heigit.bigspatialdata.oshdb.db.v0;

public class OSHDb {
	
	private OSHDb() {
		
	}
	
	
	
	public static OSHDb withDatabase(String h2path){
		return new OSHDb();
	}

}
