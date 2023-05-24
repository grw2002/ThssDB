package cn.edu.thssdb.exception;

public class DatabaseExistsException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: database already exists!";
  }
}
