package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class DropUserPlan extends LogicalPlan {

  private String userName;
  private boolean ifExists;

  public DropUserPlan(String userName, boolean ifExists) {
    super(LogicalPlanType.DROP_USER);
    this.userName = userName;
    this.ifExists = ifExists;
  }

  public String getUserName() {
    return userName;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  @Override
  public String toString() {
    return "DropUserPlan{" + "userName='" + userName + '\'' + ", ifExists=" + ifExists + '}';
  }
}
