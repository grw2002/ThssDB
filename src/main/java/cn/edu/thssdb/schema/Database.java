package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableExistsException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.PageRow;
import cn.edu.thssdb.utils.Pair;

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
        QueryTable2 result = new QueryTable2("result", table.getColumns());
        SQLParser.LiteralValueContext rValue =
            whereCondition.expression(1).comparer().literalValue();
        SQLParser.ComparatorContext op = whereCondition.comparator();
        Entry attrValue = Table.entryParse(rValue, column);
        if (rValue.K_NULL() != null) {
          if (op.NE() != null) {
            return table;
          }
          return result;
        }
        if (op.EQ() != null) {
          if (table.index.contains(attrValue)) {
            result.insert(table.index.get(attrValue));
          }
        } else if (op.NE() != null) {
          for (Pair<Entry, PageRow> index : table.index) {
            if (!index.left.equals(attrValue)) {
              result.insert(index.right);
            }
          }
        } else if (op.GT() != null) {
          Iterator iter = table.index.iterator();
          while (iter.hasNext()) {
            Pair<Entry, PageRow> item = (Pair<Entry, PageRow>) iter.next();
            if (item.left.compareTo(attrValue) <= 0) {
              continue;
            } else {
              result.insert(item.right);
              break;
            }
          }
          while (iter.hasNext()) {
            result.insert(((Pair<Entry, PageRow>) iter.next()).right);
          }
        } else if (op.LT() != null) {
          Iterator iter = table.index.iterator();
          while (iter.hasNext()) {
            Pair<Entry, PageRow> item = (Pair<Entry, PageRow>) iter.next();
            if (item.left.compareTo(attrValue) < 0) {
              result.insert(item.right);
            } else {
              break;
            }
          }
        } else if (op.GE() != null) {
          Iterator iter = table.index.iterator();
          while (iter.hasNext()) {
            Pair<Entry, PageRow> item = (Pair<Entry, PageRow>) iter.next();
            if (item.left.compareTo(attrValue) < 0) {
              continue;
            } else {
              result.insert(item.right);
              break;
            }
          }
          while (iter.hasNext()) {
            result.insert(((Pair<Entry, PageRow>) iter.next()).right);
          }
        } else if (op.LE() != null) {
          Iterator iter = table.index.iterator();
          while (iter.hasNext()) {
            Pair<Entry, PageRow> item = (Pair<Entry, PageRow>) iter.next();
            if (item.left.compareTo(attrValue) <= 0) {
              result.insert(item.right);
            } else {
              break;
            }
          }
        } else {
          throw new RuntimeException("Invalid operator: " + op.getText());
        }
        return result;
      } else {
        return QueryTable2.joinQueryTables(Collections.singletonList(table), whereCondition);
      }
    }
  }

  static boolean isEntrySatisfy(
      Entry l, SQLParser.ComparatorContext op, SQLParser.LiteralValueContext r, Column column) {
    if (r.K_NULL() != null) {
      return op.NE() != null;
    }
    Entry val = Table.entryParse(r, column);
    if (op.EQ() != null) {
      return l.compareTo(val) == 0;
    } else if (op.NE() != null) {
      return l.compareTo(val) != 0;
    } else if (op.GT() != null) {
      return l.compareTo(val) > 0;
    } else if (op.LT() != null) {
      return l.compareTo(val) < 0;
    } else if (op.GE() != null) {
      return l.compareTo(val) >= 0;
    } else if (op.LE() != null) {
      return l.compareTo(val) <= 0;
    } else {
      throw new RuntimeException("Invalid operator: " + op.getText());
    }
  }

  public QueryTable2 selectSimpleJoin(
      String tableLName,
      String tableRName,
      SQLParser.ConditionContext joinCondition,
      SQLParser.ConditionContext whereCondition) {
    Table tableL = findTableByName(tableLName);
    Table tableR = findTableByName(tableRName);
    if (tableL == null || tableR == null) {
      throw new TableNotExistException(tableLName + " " + tableRName);
    }
    if (whereCondition != null) {
      SQLParser.ColumnFullNameContext whereColumnName =
          whereCondition.expression(0).comparer().columnFullName();
      String tableName = whereColumnName.tableName().getText();
      String columnName = whereColumnName.columnName().getText();
      if (tableName.equals(tableLName)) {
        Table tmp = tableL;
        tableL = tableR;
        tableR = tmp;
      }
      // make sure tableR is the table that contains the column
      Column whereColumn = tableR.findColumnByName(columnName);
      if (whereColumn == null) {
        throw new RuntimeException("Column " + columnName + " does not exist");
      }
      SQLParser.ComparatorContext whereOp = whereCondition.comparator();
      SQLParser.LiteralValueContext whereValue =
          whereCondition.expression(1).comparer().literalValue();

      // begin join
      SQLParser.ColumnFullNameContext joinColumnNameL =
          joinCondition.expression(0).comparer().columnFullName();
      SQLParser.ColumnFullNameContext joinColumnNameR =
          joinCondition.expression(1).comparer().columnFullName();
      Column joinColumnL, joinColumnR;

      boolean flag;
      if (tableL.getTableName().equals(joinColumnNameR.tableName().getText())) {
        //        System.out.println("Mark1" + " " +
        // tableL.findColumnByName(joinColumnNameR.columnName().getText())
        //            + " " + tableR.findColumnByName(joinColumnNameL.columnName().getText()));
        flag =
            tableL.findColumnByName(joinColumnNameR.columnName().getText()).isPrimary()
                && tableR.findColumnByName(joinColumnNameL.columnName().getText()).isPrimary();
      } else {
        //        System.out.println("Mark2");
        flag =
            tableL.findColumnByName(joinColumnNameL.columnName().getText()).isPrimary()
                && tableR.findColumnByName(joinColumnNameR.columnName().getText()).isPrimary();
      }
      SQLParser.ComparatorContext joinOp = joinCondition.comparator();
      flag = flag && joinOp.EQ() != null && whereColumn.isPrimary();

      if (flag) {
        //        System.out.println("Using index join");
        Iterator<Entry> iterL = tableL.index.keyIterator();
        Iterator<Entry> iterR = tableR.index.keyIterator();
        // the most common case
        List<Entry> keysL = new ArrayList<>();
        List<Entry> keysR = new ArrayList<>();
        if (iterR.hasNext()) {
          Entry itemR = iterR.next();
          while (iterL.hasNext()) {
            Entry itemL = iterL.next();
            while (itemR.compareTo(itemL) < 0 && iterR.hasNext()) {
              itemR = iterR.next();
            }
            if (itemR.compareTo(itemL) == 0) {
              if (isEntrySatisfy(itemR, whereOp, whereValue, whereColumn)) {
                keysL.add(itemL);
                keysR.add(itemR);
              }
            }
            if (!iterR.hasNext()) {
              break;
            }
          }
        }
        List<Column> columns = new ArrayList<>();
        columns.addAll(tableL.getColumns());
        columns.addAll(tableR.getColumns());
        QueryTable2 queryTable = new QueryTable2("result", columns);
        for (int i = 0; i < keysL.size(); i++) {
          Entry keyL = keysL.get(i);
          Entry keyR = keysR.get(i);
          PageRow rowL = tableL.index.get(keyL).clone();
          PageRow rowR = tableR.index.get(keyR).clone();
          Row row = new Row();
          row.addEntries(rowL.getEntries());
          row.addEntries(rowR.getEntries());
          queryTable.insert(row);
        }
        return queryTable;
      }
      return QueryTable2.joinQueryTables(
          Collections.singletonList(
              QueryTable2.joinQueryTables(Arrays.asList(tableL, tableR), joinCondition)),
          whereCondition);
    } else {
      // begin join
      SQLParser.ColumnFullNameContext joinColumnNameL =
          joinCondition.expression(0).comparer().columnFullName();
      SQLParser.ColumnFullNameContext joinColumnNameR =
          joinCondition.expression(1).comparer().columnFullName();
      Column joinColumnL, joinColumnR;

      boolean flag;
      if (tableL.findColumnByName(joinColumnNameL.tableName().getText()) == null) {
        flag =
            tableL.findColumnByName(joinColumnNameR.columnName().getText()).isPrimary()
                && tableR.findColumnByName(joinColumnNameL.columnName().getText()).isPrimary();
      } else {
        flag =
            tableL.findColumnByName(joinColumnNameL.columnName().getText()).isPrimary()
                && tableR.findColumnByName(joinColumnNameR.columnName().getText()).isPrimary();
      }
      SQLParser.ComparatorContext joinOp = joinCondition.comparator();
      flag = flag && joinOp.EQ() != null;

      if (flag) {
        Iterator<Entry> iterL = tableL.index.keyIterator();
        Iterator<Entry> iterR = tableR.index.keyIterator();
        // the most common case
        List<Entry> keysL = new ArrayList<>();
        List<Entry> keysR = new ArrayList<>();
        if (iterR.hasNext()) {
          Entry itemR = iterR.next();
          while (iterL.hasNext()) {
            Entry itemL = iterL.next();
            while (itemR.compareTo(itemL) < 0 && iterR.hasNext()) {
              itemR = iterR.next();
            }
            if (itemR.compareTo(itemL) == 0) {
              keysL.add(itemL);
              keysR.add(itemR);
            }
            if (!iterR.hasNext()) {
              break;
            }
          }
        }
        List<Column> columns = new ArrayList<>();
        columns.addAll(tableL.getColumns());
        columns.addAll(tableR.getColumns());
        QueryTable2 queryTable = new QueryTable2("result", columns);
        for (int i = 0; i < keysL.size(); i++) {
          Entry keyL = keysL.get(i);
          Entry keyR = keysR.get(i);
          PageRow rowL = tableL.index.get(keyL).clone();
          PageRow rowR = tableR.index.get(keyR).clone();
          Row row = new Row();
          row.addEntries(rowL.getEntries());
          row.addEntries(rowR.getEntries());
          queryTable.insert(row);
        }
        return queryTable;
      }
      return QueryTable2.joinQueryTables(Arrays.asList(tableL, tableR), joinCondition);
    }
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
