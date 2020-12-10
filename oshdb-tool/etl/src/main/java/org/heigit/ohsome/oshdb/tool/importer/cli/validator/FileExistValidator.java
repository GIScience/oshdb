package org.heigit.ohsome.oshdb.tool.importer.cli.validator;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileExistValidator implements IParameterValidator{

	@Override
	public void validate(String name, String value) throws ParameterException {
		final Path path = Paths.get(value);
		if(!Files.exists(path) || !Files.isRegularFile(path))
			throw new ParameterException("File "+value+" for parameter " + name + " does not exist!");
	}
}
