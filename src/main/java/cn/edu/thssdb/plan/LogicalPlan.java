package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_USER,
    DROP_USER,
    CREATE_DB,
    DROP_DB,
    USE_DB,
    SHOW_DB,
    CREATE_TABLE,
    DROP_TABLE,
    SHOW_TABLE,
    SHOW_TABLES,
    INSERT_INTO_TABLE,
    DELETE_FROM_TABLE,
    UPDATE_TABLE,
    SELECT_FROM_TABLE,
    SIMPLE_SELECT_SINGLE_TABLE,
    SIMPLE_SELECT_JOIN_TABLE,
    ALTER_TABLE
  }
}
