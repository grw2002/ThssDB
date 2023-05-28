package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.CreateDatabasePlan;
import cn.edu.thssdb.plan.impl.DropDatabasePlan;
import cn.edu.thssdb.plan.impl.SelectPlan;
import cn.edu.thssdb.plan.impl.UseDatabasePlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  private Manager manager;

  public IServiceHandler() {
    super();
    manager = Manager.getInstance();
  }

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: implement execution logic
    LogicalPlan plan = LogicalGenerator.generate(req.statement, manager);
    System.out.println("[DEBUG] " + plan);
    switch (plan.getType()) {
      case CREATE_DB:
        manager.createDatabase(((CreateDatabasePlan) plan).getDatabaseName());
        return new ExecuteStatementResp(StatusUtil.success(), false);
      case DROP_DB:
        System.out.println("[DEBUG] " + plan);
        manager.deleteDatabase(((DropDatabasePlan) plan).getDatabaseName());
        return new ExecuteStatementResp(StatusUtil.success(), false);
      case USE_DB:
        System.out.println("[DEBUG] " + plan);
        manager.switchDatabase(((UseDatabasePlan) plan).getDatabaseName());
        return new ExecuteStatementResp(StatusUtil.success(), false);
      case SHOW_DB:
        System.out.println("[DEBUG] " + plan);
        List<String> databases = manager.showDatabases();
        String currentDbName = manager.getCurrentDatabase().getName();
        ExecuteStatementResp resp = new ExecuteStatementResp(StatusUtil.success(), true);
        resp.columnsList = Arrays.asList("Databases", "Status");
        resp.rowList =
            databases.stream()
                .map(dbName -> Arrays.asList(dbName, dbName.equals(currentDbName) ? "IN USE" : "NOT IN USE"))
                .collect(Collectors.toList());
        return resp;

      case CREATE_TABLE:
        System.out.println("[DEBUG] " + plan);
        return new ExecuteStatementResp(StatusUtil.success(), false);
      case SELECT_FROM_TABLE:
        SelectPlan selectPlan = ((SelectPlan) plan);
        QueryResult queryResult =
            manager
                .getCurrentDatabase()
                .select(selectPlan.getQueryTables(), selectPlan.getMetaInfos());
        Pair<List<String>, List<List<String>>> result = queryResult.makeResult(queryResult);
        ExecuteStatementResp res = new ExecuteStatementResp(StatusUtil.success(), true);
        res.columnsList = result.left;
        res.rowList = result.right;
        return res;
      default:
    }
    return null;
  }
}
