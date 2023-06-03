package cn.edu.thssdb.exception;

public class NotNullException extends RuntimeException {
  private String customMessage;

  public NotNullException(String customMessage) {
    this.customMessage = customMessage;
  }

  @Override
  public String getMessage() {
    return "Exception: " + customMessage;
  }
}
