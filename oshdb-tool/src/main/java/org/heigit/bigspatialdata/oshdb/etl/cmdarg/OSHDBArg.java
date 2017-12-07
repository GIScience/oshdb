package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.Parameter;
import java.io.File;

public class OSHDBArg {

  @Parameter(names = {"-db", "-oshdb", "-outputDb"}, description = "Path to output H2", required = false, order = 2)
  public File oshdb = new File("./oshdb");
}
