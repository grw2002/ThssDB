package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableExistsException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.sql.SQLParser;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database implements Serializable {

  private final String name;
  private final HashMap<String, Table> tables;
  transient ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.initTransientFields();
    //    this.storage=new Storage(this);
    recover();
  }

  public void initTransientFields() {
    this.lock = new ReentrantReadWriteLock();
  }

  public void readLock() {
    this.lock.readLock().lock();
  }

  public void readUnlock() {
    this.lock.readLock().unlock();
  }

  public void writeLock() {
    this.lock.writeLock().lock();
  }

  public void writeUnlock() {
    this.lock.writeLock().unlock();
  }

  public ReentrantReadWriteLock.ReadLock getReadLock() {
    return this.lock.readLock();
  }

  public ReentrantReadWriteLock.WriteLock getWriteLock() {
    return this.lock.writeLock();
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

  public void create(String tableName, List<Column> columns) throws RuntimeException {
    if (tables.containsKey(tableName)) {
      throw new TableExistsException();
    }
    Table newTable = new Table(this, tableName, columns);
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

  /** @param whereCondition must be primary key */
  public QueryTable2 selectSimpleSingle(
      String tableName, SQLParser.ConditionContext whereCondition) {
    Table table = findTableByName(tableName);
    if (table == null) {
      throw new TableNotExistException(tableName);
    }
    if (whereCondition == null) {
      return table;
    } else {
      String lValue = whereCondition.expression(0).getText();
      Column column = table.findColumnByName(lValue);
      if (column == null) {
        throw new RuntimeException("Column " + lValue + " does not exist");
      }
      if (column.isPrimary()) {
        SQLParser.LiteralValueContext rValue =
            whereCondition.expression(1).comparer().literalValue();
        SQLParser.ComparatorContext op = whereCondition.comparator();
        Entry value = Table.entryParse(rValue, column);
        if (rValue.K_NULL() != null) {
          // TODO: handle NULL
        }
        if (op.EQ() != null) {
          // TODO
        } else if (op.NE() != null) {
          // TODO
        } else if (op.GT() != null) {
          // TODO
        } else if (op.LT() != null) {
          // TODO
        } else if (op.GE() != null) {
          // TODO
        } else if (op.LE() != null) {
          // TODO
        } else {
          throw new RuntimeException("Invalid operator: " + op.getText());
        }
        return QueryTable2.joinQueryTables(Collections.singletonList(table), whereCondition);
      } else {
        return QueryTable2.joinQueryTables(Collections.singletonList(table), whereCondition);
      }
    }
  }

  public QueryTable2 selectSimpleJoin(
      String tableLName,
      String tableRName,
      SQLParser.ConditionContext joinCondition,
      SQLParser.ConditionContext whereCondition) {
    Table tableL = findTableByName(tableLName);
    Table tableR = findTableByName(tableRName);
    Table tmp = null; // use to swap
    if (tableL == null || tableR == null) {
      throw new TableNotExistException(tableLName + " " + tableRName);
    }
    if (whereCondition != null) {
      SQLParser.ColumnFullNameContext whereColumnName =
          whereCondition.expression(0).comparer().columnFullName();
      String tableName = whereColumnName.tableName().getText();
      String columnName = whereColumnName.columnName().getText();
      if (tableName.equals(tableRName)) {
        tmp = tableL;
        tableL = tableR;
        tableR = tmp;
      }
      // make sure tableL is the table that contains the column
      Column column = tableL.findColumnByName(columnName);
      if (column == null) {
        throw new RuntimeException("Column " + columnName + " does not exist");
      }
      if (column.isPrimary()) {
        SQLParser.LiteralValueContext rValue =
            whereCondition.expression(1).comparer().literalValue();
        SQLParser.ComparatorContext op = whereCondition.comparator();
        Entry value = Table.entryParse(rValue, column);
        if (rValue.K_NULL() != null) {
          // TODO: handle NULL
        }
        if (op.EQ() != null) {
          // TODO
        } else if (op.NE() != null) {
          // TODO
        } else if (op.GT() != null) {
          // TODO
        } else if (op.LT() != null) {
          // TODO
        } else if (op.GE() != null) {
          // TODO
        } else if (op.LE() != null) {
          // TODO
        } else {
          throw new RuntimeException("Invalid operator: " + op.getText());
        }
      } else {;
      }
    }
    return QueryTable2.joinQueryTables(Arrays.asList(tableL, tableR), joinCondition);
  }

  public void recover() {
    for (Table table : getTables()) {
      table.initTransientFields();
    }
  }

  public void persist() {
    // 遍历当前数据库中的所有表
    for (Table table : getTables()) {
      // 保存每个表的数据
      table.persist();
    }
  }

  public void quit() {
    // TODO
  }
}
