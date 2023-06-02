package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnNotExistException;
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

  /*
   utils begin
  */

  // util: automatic convert type if possible, used in alterType
  @SuppressWarnings("rawtypes")
  public Comparable convertType(Object oldValue, String newColumnType) {
    try {
      switch (newColumnType) {
        case "INT":
          if (oldValue instanceof Number) {
            return ((Number) oldValue).intValue();
          } else {
            return Integer.parseInt(oldValue.toString());
          }
        case "LONG":
          if (oldValue instanceof Number) {
            return ((Number) oldValue).longValue();
          } else {
            return Long.parseLong(oldValue.toString());
          }
        case "FLOAT":
          if (oldValue instanceof Number) {
            return ((Number) oldValue).floatValue();
          } else {
            return Float.parseFloat(oldValue.toString());
          }
        case "DOUBLE":
          if (oldValue instanceof Number) {
            return ((Number) oldValue).doubleValue();
          } else {
            return Double.parseDouble(oldValue.toString());
          }
        case "STRING":
          return oldValue.toString();
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /*
   utils end
  */
  public void addColumn(Column column) {
    this.columns.add(column);
    column.setTable(this);

    if (this.index.size() > 0) {
      Iterator<Pair<Entry, Row>> iterator = this.index.iterator();
      while (iterator.hasNext()) {
        Pair<Entry, Row> pair = iterator.next();
        Row row;

        row = pair.right; // 获取第二个元素的值
        row.addEntry(new Entry(null)); // 增加一个新的Entry
      }
    }
  }

  public void dropColumn(String columnName) {
    boolean findFlag = false;
    Column column;

    int columnIndex = -1;
    for (Iterator<Column> iterator = columns.iterator(); iterator.hasNext(); ) {
      column = iterator.next();
      columnIndex++;
      if (column.getName().equals(columnName)) {
        iterator.remove();
        findFlag = true;
        break;
      }
    }

    if (!findFlag) {
      throw new ColumnNotExistException();
    } else {
      if (this.index.size() > 0) {
        Row row;
        Iterator<Pair<Entry, Row>> iterator = this.index.iterator();

        while (iterator.hasNext()) {
          Pair<Entry, Row> pair = iterator.next();
          row = pair.right;
          row.dropEntry(columnIndex); // 增加一个新的Entry
        }
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public void alterType(String columnName, String newColumnType) {
    boolean findFlag = false;
    int columnIndex = -1;

    for (Column column : columns) {
      if (column.getName().equals(columnName)) {
        column.setType(newColumnType);
        columnIndex++;
        findFlag = true;
        break;
      }
    }

    if (!findFlag) {
      throw new ColumnNotExistException();
    } else {
      if (this.index.size() > 0) {
        Row row;
        Iterator<Pair<Entry, Row>> iterator = this.index.iterator();
        Entry oldEntry;
        Entry newEntry;
        Comparable alteredType;
        boolean ifError = false;

        while (iterator.hasNext()) {
          Pair<Entry, Row> pair = iterator.next();
          row = pair.right;
          oldEntry = row.entries.get(columnIndex);
          alteredType = convertType(oldEntry.value, newColumnType);

          if (!ifError && alteredType == null) {
            System.out.println(
                "Cannot convert from "
                    + oldEntry.value.getClass().getSimpleName()
                    + " to "
                    + newColumnType
                    + ", set to null.");
            ifError = true;
          }
          newEntry = new Entry(alteredType);
          row.alterEntryType(columnIndex, newEntry);
        }
      }
    }
  }

  public void alterName(String columnName, String newColumnName) {
    boolean findFlag = false;
    for (Column column : columns) {
      if (column.getName().equals(columnName)) {
        column.setName(newColumnName);
        findFlag = true;
        break;
      }
    }

    if (!findFlag) {
      throw new ColumnNotExistException();
    }
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
