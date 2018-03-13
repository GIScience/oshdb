package org.heigit.bigspatialdata.oshdb.tool.importer.extract.cli;

import java.nio.file.Path;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.CommonArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.DistributableArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.FileExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.TimeValidity;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class ExtractArgs {
  @ParametersDelegate
  public CommonArgs common = new CommonArgs();

  @ParametersDelegate
  public DistributableArgs distribute = new DistributableArgs();

  @Parameter(names = {
      "--pbf" }, description = "path to pbf-File to import", validateWith = FileExistValidator.class, required = true, order = 0)
  public Path pbf;
  
  @Parameter(names = {"--md5"}, description="MD5 checksum")
  public String md5 = "";
  
  @Parameter(names = {"--poly"}, description="extract region",  validateWith = FileExistValidator.class)
  public Path polyFile;
  
  @Parameter(names = {"--bbox"}, description="extract_region")
  public String bbox = "";
  
  @Parameter(names = {"--timevalidity_from"}, description="first valid timestamp in isodate format", required = true, validateWith=TimeValidity.class)
  public String timeValidityFrom; //cc-by-sa 2007-10-07
  
  @Parameter(names = {"--timevalidity_to"}, description="latest valid timestamp in isodate format", validateWith=TimeValidity.class)
  public String timeValidityTo = null; //

  @Parameter(names = { "--overwrite" }, description = "overwrite existing files", order = 1)
  public boolean overwrite = false;
}
