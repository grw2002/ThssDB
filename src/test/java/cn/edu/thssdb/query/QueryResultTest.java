package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.DatabaseExistsException;
import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.SelectPlan;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.StatusUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class QueryResultTest {
  private Manager manager;

  static final Column[] columns1 =
      new Column[] {
        new Column("id", ColumnType.INT, 1, true, 0),
        new Column("name", ColumnType.STRING, 0, false, 128),
        new Column("age", ColumnType.INT, 0, false, 0),
      };

  static final Row[] rows1 =
      new Row[] {
        new Row(new Entry[] {new Entry(1), new Entry("name1"), new Entry(20)}),
        new Row(new Entry[] {new Entry(2), new Entry("name2"), new Entry(21)}),
        new Row(new Entry[] {new Entry(3), new Entry("name3"), new Entry(22)}),
        new Row(new Entry[] {new Entry(4), new Entry("name4"), new Entry(23)}),
        new Row(new Entry[] {new Entry(5), new Entry("name5"), new Entry(24)}),
        new Row(new Entry[] {new Entry(6), new Entry("name6"), new Entry(25)}),
        new Row(new Entry[] {new Entry(7), new Entry("name7"), new Entry(26)}),
      };

  static final Column[] columns2 =
      new Column[] {
        new Column("id", ColumnType.INT, 1, true, 0),
        new Column("location", ColumnType.STRING, 0, false, 128),
        new Column("phone", ColumnType.INT, 0, false, 0),
      };
  static final Row[] rows2 =
      new Row[] {
        new Row(new Entry[] {new Entry(1), new Entry("location1"), new Entry(100000)}),
        new Row(new Entry[] {new Entry(2), new Entry("location2"), new Entry(100001)}),
        new Row(new Entry[] {new Entry(3), new Entry("location3"), new Entry(100002)}),
        new Row(new Entry[] {new Entry(4), new Entry("location4"), new Entry(100003)}),
        new Row(new Entry[] {new Entry(5), new Entry("location5"), new Entry(100004)}),
      };

  @Before
  public void setUp() {
    manager = Manager.getInstance();
    try {
      manager.createDatabase("db1");
    } catch (DatabaseExistsException e) {
      manager.switchDatabase("db1");
    }
    Database db = manager.getCurrentDatabase();

    if (db.findTableByName("table1") != null) {
      db.drop("table1");
    }
    if (db.findTableByName("table2") != null) {
      db.drop("table2");
    }
    db.create("table1", columns1);
    Table table1 = db.findTableByName("table1");
    table1.insert(rows1);

    db.create("table2", columns2);
    Table table2 = db.findTableByName("table2");
    table2.insert(rows2);
  }

  private ExecuteStatementResp executeStatementResp(String sql) {
    LogicalPlan plan = LogicalGenerator.generate(sql, manager);
    assertEquals(plan.getType(), LogicalPlan.LogicalPlanType.SELECT_FROM_TABLE);
    System.out.println("[DEBUG] " + plan);
    SelectPlan selectPlan = ((SelectPlan) plan);
    QueryResult queryResult =
        manager
            .getCurrentDatabase()
            .select(
                selectPlan.getQueryTables(),
                selectPlan.getMetaInfos(),
                selectPlan.getJoinCondition());
    Pair<List<String>, List<List<String>>> result = QueryResult.makeResult(queryResult);
    ExecuteStatementResp res = new ExecuteStatementResp(StatusUtil.success(), true);
    res.columnsList = result.left;
    res.rowList = result.right;
    return res;
  }

  @Test
  public void testSelectFromOneTable() {
    String[] attrs = new String[] {"age", "name"};
    String sql = "SELECT " + String.join(",", attrs) + " FROM table1;";
    ExecuteStatementResp res = executeStatementResp(sql);
    System.out.println("[DEBUG] " + res.columnsList + res.rowList);

    Database db = manager.getCurrentDatabase();
    Table table1 = db.findTableByName("table1");
    assertEquals(res.columnsList.size(), 2);
    assertEquals(String.join("#", res.columnsList), String.join("#", attrs));
    assertEquals(res.rowList.size(), 7);
    for (int i = 0; i < res.rowList.size(); i++) {
      List<String> strings = res.rowList.get(i);
      assertEquals(strings.size(), 2);
      List<Entry> entries = rows1[i].getEntries();
      for (int j = 0; j < strings.size(); j++) {
        assertEquals(
            entries.get(table1.findColumnIndexByName(attrs[j])).toString(), strings.get(j));
      }
    }
  }

  @Test
  public void testSelectFromMultipleTables() {
    String[] attrs = new String[] {"name", "age", "age", "name", "id", "phone", "location"};
    String sql = "SELECT " + String.join(",", attrs) + " FROM table1,table2;";
    ExecuteStatementResp res = executeStatementResp(sql);
    System.out.println("[DEBUG] " + res.columnsList + res.rowList);

    Database db = manager.getCurrentDatabase();
    Table table1 = db.findTableByName("table1");
    Table table2 = db.findTableByName("table2");
    assertEquals(res.columnsList.size(), 7);
    assertEquals(String.join("#", attrs), String.join("#", res.columnsList));
    assertEquals(res.rowList.size(), 35);
    for (int j = 0; j < rows2.length; j++) {
      for (int i = 0; i < rows1.length; i++) {
        List<String> strings = res.rowList.get(j * rows1.length + i);
        assertEquals(strings.size(), 7);
        List<Entry> entries1 = rows1[i].getEntries();
        List<Entry> entries2 = rows2[j].getEntries();
        for (int k = 0; k < strings.size(); k++) {
          if (k < 5) {
            assertEquals(
                entries1.get(table1.findColumnIndexByName(attrs[k])).toString(), strings.get(k));
          } else {
            assertEquals(
                entries2.get(table2.findColumnIndexByName(attrs[k])).toString(), strings.get(k));
          }
        }
      }
    }
  }

  //  @Test
  //  public void testTest() {
  //    String[] attrs = new String[] {"age", "name"};
  ////    String sql = "SELECT " + String.join(",", attrs) + " FROM table1;";
  //    String sql="SELECT table1.id,table1.name,table2.location FROM table1,table2;";
  //    ExecuteStatementResp res = executeStatementResp(sql);
  //    System.out.println("[DEBUG] " + res.columnsList + res.rowList);
  //
  //    Database db = manager.getCurrentDatabase();
  //    Table table1 = db.findTableByName("table1");
  //    assertEquals(res.columnsList.size(), 2);
  //    assertEquals(String.join("#", res.columnsList), String.join("#", attrs));
  //    assertEquals(res.rowList.size(), 7);
  //    for (int i = 0; i < res.rowList.size(); i++) {
  //      List<String> strings = res.rowList.get(i);
  //      assertEquals(strings.size(), 2);
  //      List<Entry> entries = rows1[i].getEntries();
  //      for (int j = 0; j < strings.size(); j++) {
  //        assertEquals(
  //            entries.get(table1.findColumnIndexByName(attrs[j])).toString(), strings.get(j));
  //      }
  //    }
  //  }
}
