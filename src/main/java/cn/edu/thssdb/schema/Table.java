package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].getPrimary() != 0) {
        primaryIndex = i;
        break;
      }
      columns[i].setTable(this);
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

  public void insertNameValue(List<String> columnNames, List<List<String>> values) {
    List<String> allColumnNames =
        getColumns().stream().map(Column::getName).collect(Collectors.toList());
    if (columnNames.isEmpty()) {
      columnNames = allColumnNames;
    }

    if (!new HashSet<>(allColumnNames).containsAll(columnNames)) {
      throw new RuntimeException("Some fields don't exist");
    }

    for (List<String> valueList : values) {
      if (valueList.size() != columnNames.size()) {
        throw new RuntimeException("The number of values does not match the number of columns");
      }

      Entry[] entries = new Entry[allColumnNames.size()];
      int valueIndex = 0;

      for (int i = 0; i < allColumnNames.size(); i++) {
        String columnName = allColumnNames.get(i);
        Column column = findColumnByName(columnName);

        ColumnType columnType = column.getType();

        Comparable value;
        if (columnNames.contains(columnName)) {
          // The column is specified, use the corresponding value
          switch (columnType) {
            case INT:
              value = Integer.parseInt(valueList.get(valueIndex));
              break;
            case LONG:
              value = Long.parseLong(valueList.get(valueIndex));
              break;
            case FLOAT:
              value = Float.parseFloat(valueList.get(valueIndex));
              break;
            case DOUBLE:
              value = Double.parseDouble(valueList.get(valueIndex));
              break;
            case STRING:
              value = valueList.get(valueIndex);
              break;
            default:
              throw new RuntimeException(
                  "Unsupported column type or value: "
                      + columnType
                      + " "
                      + valueList.get(valueIndex));
          }
          valueIndex++;
        } else {
          // The column is not specified, use a default value
          if (findColumnByName(columnName).getNotNull()) {
            throw new RuntimeException("Field '" + columnName + "' cannot be null");
          }
          switch (columnType) {
            case INT:
              value = 0;
              break;
            case LONG:
              value = 0L;
              break;
            case FLOAT:
              value = 0.0f;
              break;
            case DOUBLE:
              value = 0.0;
              break;
            case STRING:
              value = "";
              break;
            default:
              throw new RuntimeException("Unsupported column type: " + columnType);
          }
        }

        entries[i] = new Entry(value);
      }

      Row row = new Row(entries);
      insert(new Row[] {row});
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
