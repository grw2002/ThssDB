package cn.edu.thssdb.exception;

public class TypeNotMatchException extends RuntimeException {
  private String customMessage;

  public TypeNotMatchException(String customMessage) {
    this.customMessage = customMessage;
  }

  @Override
  public String getMessage() {
    return "Type Not Match: " + customMessage;
  }
}
