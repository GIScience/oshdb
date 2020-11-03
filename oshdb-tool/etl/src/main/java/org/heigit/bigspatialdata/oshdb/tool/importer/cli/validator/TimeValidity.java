package org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator;

import org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

public class TimeValidity implements IParameterValidator{

  @Override
  public void validate(String name, String value) throws ParameterException {
    try {
      IsoDateTimeParser.parseIsoDateTime(value);
    } catch (Exception e) {
      throw new ParameterException(e.getMessage());
    }    
  }
  
}
