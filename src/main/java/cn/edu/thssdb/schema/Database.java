package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableExistsException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.QueryTable2;
import cn.edu.thssdb.sql.SQLParser;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database implements Serializable {

  private final String name;
  private final HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  public String getName() {
    return name;
  }

  public Collection<Table> getTables() {
    return this.tables.values();
  }

  public List<String> getTablesName() {
    return new ArrayList<>(tables.keySet());
  }

  public List<Column> getTableColumns(String tableName) {
    Table table = findTableByName(tableName);

    if (table == null) {
      throw new RuntimeException("Table " + tableName + " does not exist");
    }
    return table.getColumns();
  }

  public Table findTableByName(String tableName) {
    return tables.get(tableName);
  }

  private void persist() {
    // TODO
  }

  public void create(String tableName, Column[] columns) throws RuntimeException {
    if (tables.containsKey(tableName)) {
      throw new TableExistsException();
    }
    Table newTable = new Table(this.name, tableName, columns);
    tables.put(tableName, newTable);
  }

  public void drop(String tableName) throws RuntimeException {
    if (!tables.containsKey(tableName)) {
      throw new TableNotExistException(tableName);
    }
    tables.remove(tableName);
  }

  @Deprecated
  public QueryResult select(
      List<QueryTable> queryTables,
      List<MetaInfo> metaInfos,
      String joinCondition,
      String whereCondition) {
    return new QueryResult(queryTables, metaInfos, joinCondition, whereCondition);
  }

  public QueryTable2 select(
      List<SQLParser.TableQueryContext> tableQuerys,
      SQLParser.MultipleConditionContext whereConditions)
      throws RuntimeException {
    List<QueryTable2> queryTables = new ArrayList<>();
    for (SQLParser.TableQueryContext tableQuery : tableQuerys) {
      List<QueryTable2> subtables = new ArrayList<>();
      for (SQLParser.TableNameContext tableNameContext : tableQuery.tableName()) {
        String tableName = tableNameContext.getText();
        Table table = findTableByName(tableName);
        if (table == null) {
          throw new TableNotExistException(tableName);
        }
        subtables.add(table);
      }
      SQLParser.MultipleConditionContext joinConditions = tableQuery.multipleCondition();
      if (joinConditions != null) {
        queryTables.add(QueryTable2.joinQueryTables(subtables, joinConditions));
      } else {
        queryTables.add(QueryTable2.joinQueryTables(subtables));
      }
    }
    if (whereConditions == null) {
      return QueryTable2.joinQueryTables(queryTables);
    } else {
      return QueryTable2.joinQueryTables(queryTables, whereConditions);
    }
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
