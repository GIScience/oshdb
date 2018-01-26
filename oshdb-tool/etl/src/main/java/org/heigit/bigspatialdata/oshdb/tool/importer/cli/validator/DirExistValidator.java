package org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class DirExistValidator implements IParameterValidator{

	@Override
	public void validate(String name, String value) throws ParameterException {
		final Path path = Paths.get(value);
		try {
      Files.createDirectories(path);
    } catch (IOException e) {
      throw new ParameterException("Directory "+value+" for parameter " + name + " could not created");
    }
		if(!Files.exists(path) && Files.isDirectory(path))
		  throw new ParameterException("Directory "+value+" for parameter " + name + " does not exist!");
	}

}
