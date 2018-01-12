package org.heigit.bigspatialdata.oshdb.tool.etl;

/**
 * Holds important filenames that may be created during ETL.
 */
public enum EtlFiles {

  /**
   * Temporary DB created during extraction to speed up Transformation.
   */
  E_TEMPRELATIONS("temp_relations", ".mv.db");
  private final String file;
  private final String extension;

  EtlFiles(String file, String extension) {
    this.file = file;
    this.extension = extension;
  }

  @Override
  public String toString() {
    return file + extension;
  }

  public String getName() {
    return file;
  }

  public String getExtension() {
    return extension;
  }
}
