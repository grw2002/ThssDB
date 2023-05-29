package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class CreateUserPlan extends LogicalPlan {

  private String userName;
  private String password;

  public CreateUserPlan(String userName, String password) {
    super(LogicalPlanType.CREATE_USER);
    this.userName = userName;
    this.password = password;
  }

  public String getUserName() {
    return userName;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public String toString() {
    return "CreateUserPlan{"
        + "userName='"
        + userName
        + '\''
        + ", password='"
        + password
        + '\''
        + '}';
  }
}
