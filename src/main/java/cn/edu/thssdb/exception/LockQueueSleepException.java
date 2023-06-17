package cn.edu.thssdb.exception;

public class LockQueueSleepException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: Exceptions when session sleep in lockQueue!";
  }
}
