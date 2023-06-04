package cn.edu.thssdb.exception;

public class ColumnNotExistException extends RuntimeException {

  String columnName = "";

  public ColumnNotExistException(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public String getMessage() {
    return "Exception: column " + columnName + " doesn't exist!";
  }
}
