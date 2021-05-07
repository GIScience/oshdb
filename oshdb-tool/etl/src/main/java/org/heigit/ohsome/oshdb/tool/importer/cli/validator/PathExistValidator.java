package org.heigit.ohsome.oshdb.tool.importer.cli.validator;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class PathExistValidator implements IParameterValidator {

  @Override
  public void validate(String name, String value) throws ParameterException {

    if (!Files.exists(Paths.get(value))) {
      throw new ParameterException("File " + value + " for parameter " + name + " does not exist!");
    }
  }
}