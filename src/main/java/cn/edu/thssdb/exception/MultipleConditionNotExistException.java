package cn.edu.thssdb.exception;

public class MultipleConditionNotExistException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: MultiCondition in DELETE doesnt exist!";
  }
}
