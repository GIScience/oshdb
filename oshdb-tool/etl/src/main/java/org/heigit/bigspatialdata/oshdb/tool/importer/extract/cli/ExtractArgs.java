package org.heigit.bigspatialdata.oshdb.tool.importer.extract.cli;

import java.nio.file.Path;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.CommonArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.DistributableArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.FileExistValidator;

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

  @Parameter(names = { "--overwrite" }, description = "overwrite existing files", order = 1)
  public boolean overwrite = false;
}
