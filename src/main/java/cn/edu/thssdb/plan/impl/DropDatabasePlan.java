package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class DropDatabasePlan extends LogicalPlan {

  private String databaseName;
  private boolean ifExists;

  public DropDatabasePlan(String databaseName, boolean ifExists) {
    super(LogicalPlanType.DROP_DB);
    this.databaseName = databaseName;
    this.ifExists = ifExists;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  @Override
  public String toString() {
    return "DropDatabasePlan{"
        + "databaseName='"
        + databaseName
        + '\''
        + ", ifExists="
        + ifExists
        + '}';
  }
}
