package org.heigit.ohsome.oshdb.tool.importer.cli.validator;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.heigit.ohsome.oshdb.util.time.IsoDateTimeParser;

public class TimeValidity implements IParameterValidator {
  @Override
  public void validate(String name, String value) throws ParameterException {
    try {
      IsoDateTimeParser.parseIsoDateTime(value);
    } catch (Exception e) {
      throw new ParameterException(e.getMessage());
    }
  }
}
