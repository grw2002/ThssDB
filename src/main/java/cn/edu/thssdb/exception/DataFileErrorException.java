package cn.edu.thssdb.exception;

public class DataFileErrorException extends RuntimeException {

  private String dataTableName;

  public DataFileErrorException(String dataTableName) {
    this.dataTableName = dataTableName;
  }

  @Override
  public String getMessage() {
    return "Exception: Error with data file " + dataTableName+ ".data !";
  }
}
