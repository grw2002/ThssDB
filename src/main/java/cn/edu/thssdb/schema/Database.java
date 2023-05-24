package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableExistsException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
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
      throw new TableNotExistException();
    }
    tables.remove(tableName);
  }

  public QueryResult select(QueryResult pendingQuery) {
    QueryResult queryResult = pendingQuery;
    Queue<LinkedList<Row>> oldrows = new LinkedList<>();
    Queue<LinkedList<Row>> newrows = new LinkedList<>();
    Queue<LinkedList<Row>> tmp;
    oldrows.add(new LinkedList<>());
    for (QueryTable queryTable : pendingQuery.queryTables) {
      while (queryTable.hasNext()) {
        Row row = queryTable.next();
        while (!oldrows.isEmpty()) {
          LinkedList<Row> rows = oldrows.remove();
          LinkedList<Row> newRow = new LinkedList<>(rows);
          newRow.add(row);
          newrows.add(newRow);
        }
        // Swap
        tmp = oldrows;
        oldrows = newrows;
        newrows = tmp;
        newrows.clear();
      }
    }
    for (LinkedList<Row> oldrow : oldrows) {
      queryResult.result.right.add(
          QueryResult.filterRow(QueryResult.combineRow(oldrow), pendingQuery.index));
    }
    return queryResult;
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
