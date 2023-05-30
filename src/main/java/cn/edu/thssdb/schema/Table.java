package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row>, Serializable {
  ReentrantReadWriteLock lock;
  public String databaseName;
  public String tableName;
  private ArrayList<Column> columns;
  public transient BPlusTree<Entry, Row> index;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.index = new BPlusTree<>();
    for (Column column : this.columns) {
      column.setTable(this);
    }
  }

  public void addColumn(Column column) {
    this.columns.add(column);
    column.setTable(this);
  }

  public List<Column> getColumns() {
    return columns;
  }

  private void recover() {
    // TODO
  }

  public void insert(Row[] rows) {
    // TODO
    for (Row row : rows) {
      Entry primaryKey = row.entries.get(primaryIndex);
      if (!index.contains(primaryKey)) {
        index.put(row.entries.get(primaryIndex), row);
      }
    }
  }

  public void delete(Entry[] primaryKeys) {
    // TODO
    for (Entry primaryKey : primaryKeys) {
      if (index.contains(primaryKey)) {
        index.remove(primaryKey);
      }
    }
  }

  public void update(Entry[] primaryKeys, int columnIndexToUpdate, Entry newValue) {
    // TODO
    for (Entry primaryKey : primaryKeys) {
      //      Row row = index.get(primaryKey);
      //      row.entries.set(columnIndexToUpdate, newValue);
      //      index.update(primaryKey, row);
      /** ArrayList.get得到的是对象的引用，所以可以直接这么写 */
      index.get(primaryKey).entries.set(columnIndexToUpdate, newValue);
    }
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  public int findColumnIndexByName(String name) {
    Column column = findColumnByName(name);
    if (column != null) {
      return column.getIndex();
    }
    return -1;
  }

  public Column findColumnByName(String name) {
    //    System.out.println("find Column By Name:" + name + columns.toString());
    for (Column column : columns) {
      if (column.getName().equals(name)) {
        return column;
      }
    }
    return null;
  }

  private static class TableIterator implements Iterator<Row> {
    private final Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
