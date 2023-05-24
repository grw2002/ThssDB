package cn.edu.thssdb.exception;

public class TableExistsException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: table already exists!";
  }
}
