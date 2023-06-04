package cn.edu.thssdb.exception;

/** ************* [Exception] 表不存在 ************* */
public class TableNotExistException extends RuntimeException {

  String tableName = "";

  public TableNotExistException(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public String getMessage() {
    return "Exception: table " + tableName + " doesn't exist!";
  }
}
