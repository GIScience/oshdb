package org.heigit.bigspatialdata.oshdb.tool.importer.transform.cli;

import java.nio.file.Path;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.CommonArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.DistributableArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.FileExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.cli.validator.TransformStepValidator;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.validators.PositiveInteger;

public class TransformArgs {
  @ParametersDelegate
  public CommonArgs common = new CommonArgs();
  
  @ParametersDelegate
  public DistributableArgs distribute = new DistributableArgs();

  @Parameter(names = {"--pbf" }, description = "path to pbf-File to import", validateWith = FileExistValidator.class, required = true, order = 0)
  public Path pbf;

  @Parameter(names = {"-s", "--step" }, description = "step for transformation (all|node|way|relation)", validateWith = TransformStepValidator.class,  order = 1)
  public String step = "all";
  
  @Parameter(names = {"-z", "--maxZoom" }, description = "maximal zoom level", validateWith = PositiveInteger.class,  order = 2)
  public int maxZoom = 15;
  
  @Parameter(names = { "--overwrite" }, description = "overwrite existing files", order = 3)
  public boolean overwrite = false;
}
