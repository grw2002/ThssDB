package cn.edu.thssdb.exception;

public class LogFileException extends RuntimeException{
  private String databaseName;

  public LogFileException(String databaseName) {
    this.databaseName = databaseName;
  }
  @Override
  public String getMessage() {
    return "Exception: Access to logFile of database [" + databaseName + "] failed!";
  }
}
