package org.heigit.bigspatialdata.oshdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OSHDbH2 implements OSHDb{
	
	
	
	
	private OSHDbH2(){}
	
	
	public static OSHDb getDatabase(String h2Path) throws ClassNotFoundException, SQLException{
		Class.forName("org.h2.Driver");
		
		Connection conn = DriverManager.getConnection("jdbc:h2:"+h2Path, "sa", "");
		
		
		return null;
	}


	@Override
	public void close() throws IOException {
		
		
	}

}
