package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.DataFileErrorException;
import cn.edu.thssdb.exception.NotNullException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.query.QueryTable2;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Table extends QueryTable2 {
  ReentrantReadWriteLock lock;
  public String databaseName;
  private int primaryIndex;
  private String tableName;
  public transient BPlusTree<Entry, Row> index;

  public String getTableName() {
    return tableName;
  }

  public Table(String databaseName, String tableName, Column[] columns) {
    // TODO
    super(tableName, columns); // 调用父类构造函数
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.index = new BPlusTree<>();
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].getPrimary() != 0) {
        primaryIndex = i;
      }
      columns[i].setTable(this);
    }
  }

  @Override
  public void addColumn(Column column) {
    // TODO
    super.addColumn(column);
    column.setTable(this);
  }

  /*
   utils begin
  */

  // util: save index into file
  public void saveTableDataToFile() {
    String fileName = this.tableName + ".data";

    try (FileOutputStream fos = new FileOutputStream(fileName);
        GZIPOutputStream gos = new GZIPOutputStream(fos);
        ObjectOutputStream oos = new ObjectOutputStream(gos)) {

      oos.writeObject(this.index);

    } catch (IOException e) {
      // Handle the exception
      throw new DataFileErrorException(this.tableName + "," + e.toString());
    }
  }

  // util: load index from file
  public void loadTableDataFromFile() {
    String fileName = this.tableName + ".data";
    Path loadPath = Paths.get(fileName);

    if (!Files.exists(loadPath)) {
      System.out.println("No such file:" + loadPath);
      return;
    }
    try {
      if (Files.size(loadPath) == 0) {
        System.out.println("Empty data file. No existing index.");
        this.index = new BPlusTree<>(); // 如果文件为空，初始化 index 为一个空的 BPlusTree
        return;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    try (FileInputStream fis = new FileInputStream(fileName);
        GZIPInputStream gis = new GZIPInputStream(fis);
        ObjectInputStream ois = new ObjectInputStream(gis)) {

      Object fileContent = ois.readObject();
      if (fileContent != null) {
        if (fileContent instanceof BPlusTree) {
          this.index = (BPlusTree<Entry, Row>) fileContent;
          System.out.println("loading...");
        } else {
          System.out.println("Invalid data file content. Expected BPlusTree<Entry, Row>.");
        }
      } else {
        System.out.println("Empty data file. Initializing index as an empty BPlusTree.");
        this.index = new BPlusTree<>();
      }
      System.out.println("loaded");
    } catch (IOException | ClassNotFoundException e) {
      throw new DataFileErrorException(this.tableName + "," + e.toString());
    }
  }

  // util: parse string and return entry
  public static Entry entryParse(
      String value, String columnName, ColumnType columnType, boolean notNull) {
    if (value.equalsIgnoreCase("null")) {
      if (notNull) throw new NotNullException(columnName);
      return new Entry(null);
    }

    switch (columnType) {
      case INT:
        return new Entry(Integer.valueOf(value));
      case LONG:
        return new Entry(Long.valueOf(value));
      case FLOAT:
        return new Entry(Float.valueOf(value));
      case DOUBLE:
        return new Entry(Double.valueOf(value));
      case STRING:
        if (value.startsWith("\'") && value.endsWith("\'")) {
          value = value.substring(1, value.length() - 1);
        }
        return new Entry(value);
      default:
        return null;
    }
  }

  // util: get all rows
  public List<String> getAllRowsInfo() {
    List<String> allRows = new ArrayList<>();

    BPlusTreeIterator<Entry, Row> iter = index.iterator();
    while (iter.hasNext()) {
      Pair<Entry, Row> pair = iter.next();
      Row row = (Row) pair.right;
      allRows.add(row.toString());
    }
    return allRows;
  }

  /*
   utils end
  */

  public void dropColumn(String columnName) {
    boolean findFlag = false;
    Column column;

    int columnIndex = -1;
    for (Iterator<Column> iterator = columns.iterator(); iterator.hasNext(); ) {
      column = iterator.next();
      columnIndex++;
      if (column.getName().equals(columnName)) {
        iterator.remove();
        updateColumnIndex();
        findFlag = true;
        break;
      }
    }

    if (!findFlag) {
      throw new ColumnNotExistException(columnName);
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

  // util: initTransientFields BPlusTree & Map
  public void initTransientFields() {
    this.index = new BPlusTree<>();
    this.columnIndex = new HashMap<>();
    updateColumnIndex();
  }

  public void alterType(String columnName, String newColumnType) {
    boolean findFlag = false;
    int columnIndex = -1;
    boolean notNull = false;

    for (Column column : columns) {
      if (column.getName().equals(columnName)) {
        column.setType(newColumnType);
        notNull = column.getNotNull();
        columnIndex++;
        findFlag = true;
        break;
      }
    }

    if (!findFlag) {
      throw new ColumnNotExistException(columnName);
    } else {
      if (this.index.size() > 0) {
        Row row;
        Iterator<Pair<Entry, Row>> iterator = this.index.iterator();
        Entry oldEntry;
        Entry newEntry;
        boolean ifError = false;

        while (iterator.hasNext()) {
          Pair<Entry, Row> pair = iterator.next();
          row = pair.right;
          oldEntry = row.entries.get(columnIndex);
          try {
            newEntry =
                entryParse(
                    oldEntry.value.toString(),
                    columnName,
                    ColumnType.valueOf(newColumnType),
                    notNull);
          } catch (Exception e) {
            if (!ifError) {
              System.out.println(
                  "Cannot convert from "
                      + oldEntry.value.getClass().getSimpleName()
                      + " to "
                      + newColumnType
                      + ", set to null.");
              ifError = true;
            }
            newEntry = new Entry(null);
          }
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
      throw new ColumnNotExistException(columnName);
    }
  }

  private void recover() {
    // TODO
  }

  @Override
  public void insert(Row row) {
    Entry primaryKey = row.entries.get(primaryIndex);
    if (!index.contains(primaryKey)) {
      index.put(row.entries.get(primaryIndex), row);
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

        if (columnNames.contains(columnName)) {
          entries[i] =
              new Entry(
                  entryParse(
                      valueList.get(valueIndex),
                      column.getName(),
                      column.getType(),
                      column.getNotNull()));
          if (column.getPrimary() == 1) {
            Entry primaryKey = entries[i];
            // 检查主键是否已经存在
            if (index.contains(primaryKey)) {
              throw new RuntimeException("Duplicate primary key '" + primaryKey + "'");
            }
          }
          valueIndex++;
        } else {
          // The column is not specified, use a default value
          if (findColumnByName(columnName).getNotNull()) {
            throw new RuntimeException("Field '" + columnName + "' cannot be null");
          }
          switch (column.getType()) {
            case INT:
              entries[i] = new Entry(0);
              break;
            case LONG:
              entries[i] = new Entry(0L);
              break;
            case FLOAT:
              entries[i] = new Entry(0.0f);
              break;
            case DOUBLE:
              entries[i] = new Entry(0.0);
              break;
            case STRING:
              entries[i] = new Entry("");
              break;
            default:
              throw new RuntimeException("Unsupported column type");
          }
        }
      }

      Row row = new Row(entries);
      insert(new Row[] {row});
    }
  }

  public void deleteWithConditions(List<String> conditions) {
    // Get the only condition from the list
    String condition = conditions.get(0);

    // Parse the condition
    String[] parts = condition.split(" ");
    String columnName = parts[0];
    String operator = parts[1];
    String value = parts[2];
    if (value.startsWith("'") && value.endsWith("'")) {
      value = value.substring(1, value.length() - 1); // drop the ' in 'String'
    }

    // Prepare a list for the primary keys of the rows to be deleted
    List<Entry> toDelete = new ArrayList<>();

    // Iterate over all rows
    BPlusTreeIterator<Entry, Row> iterator = index.iterator();
    while (iterator.hasNext()) {
      Pair<Entry, Row> pair = iterator.next();
      Row row = pair.right;

      // Get the value in the column for this row
      Entry columnValueEntry = row.getEntries().get(columnIndex.get(columnName));
      String columnValue =
          columnValueEntry
              .toString(); // Assuming Entry has a toString method that returns its value

      // Check if the condition is satisfied for this row
      switch (operator) {
          // '=', '<>', '<', '>', '<=', '>='
        case "=":
          if (columnValue.equals(value)) {
            toDelete.add(pair.left);
          }
          break;
        case "<>":
          if (!columnValue.equals(value)) {
            toDelete.add(pair.left);
          }
          break;
        case "<":
          if (columnValue.compareTo(value) <= 0) {
            toDelete.add(pair.left);
          }
          break;
        case ">":
          if (columnValue.compareTo(value) >= 0) {
            toDelete.add(pair.left);
          }
          break;
        case "<=":
          if (columnValue.compareTo(value) < 0) {
            toDelete.add(pair.left);
          }
          break;
        case ">=":
          if (columnValue.compareTo(value) > 0) {
            toDelete.add(pair.left);
          }
          break;
        default:
          throw new RuntimeException("Invalid operator: " + operator);
      }
    }

    // Delete the rows
    delete(toDelete.toArray(new Entry[0]));
  }

  public void delete(Entry[] primaryKeys) {
    // TODO
    for (Entry primaryKey : primaryKeys) {
      if (index.contains(primaryKey)) {
        index.remove(primaryKey);
      }
    }
  }

  public void updateWithConditions(String columnName, String newValue, List<String> conditions) {
    // Get the column index to update
    Integer columnIndexToUpdate = columnIndex.get(columnName);
    if (columnIndexToUpdate == null) {
      throw new ColumnNotExistException(columnName);
    }

    // Parse new value to Entry according to the column type
    Entry newEntryValue;
    switch (getColumns().get(columnIndexToUpdate).getType()) {
      case INT:
        newEntryValue = new Entry(Integer.parseInt(newValue));
        break;
      case LONG:
        newEntryValue = new Entry(Long.parseLong(newValue));
        break;
      case FLOAT:
        newEntryValue = new Entry(Float.parseFloat(newValue));
        break;
      case DOUBLE:
        newEntryValue = new Entry(Double.parseDouble(newValue));
        break;
      case STRING:
        newEntryValue = new Entry(newValue);
        break;
      default:
        throw new RuntimeException("Unsupported column type");
    }

    // Prepare a list for the primary keys of the rows to be updated
    List<Entry> toUpdate = new ArrayList<>();

    // Process the conditions
    for (String condition : conditions) {
      String[] parts = condition.split(" ");
      String conditionColumnName = parts[0];
      String operator = parts[1];
      String value = parts[2];
      if (value.startsWith("'") && value.endsWith("'")) {
        value = value.substring(1, value.length() - 1);
      }

      // Iterate over all rows
      BPlusTreeIterator<Entry, Row> iterator = index.iterator();
      while (iterator.hasNext()) {
        Pair<Entry, Row> pair = iterator.next();
        Row row = pair.right;

        // Get the value in the column for this row
        Entry columnValueEntry = row.getEntries().get(columnIndex.get(conditionColumnName));
        String columnValue = columnValueEntry.toString();

        // Check if the condition is satisfied for this row
        switch (operator) {
          case "=":
            if (columnValue.equals(value)) {
              toUpdate.add(pair.left);
            }
            break;
          case "<>":
            if (!columnValue.equals(value)) {
              toUpdate.add(pair.left);
            }
            break;
          case "<":
            if (columnValue.compareTo(value) < 0) {
              toUpdate.add(pair.left);
            }
            break;
          case ">":
            if (columnValue.compareTo(value) > 0) {
              toUpdate.add(pair.left);
            }
            break;
          case "<=":
            if (columnValue.compareTo(value) <= 0) {
              toUpdate.add(pair.left);
            }
            break;
          case ">=":
            if (columnValue.compareTo(value) >= 0) {
              toUpdate.add(pair.left);
            }
            break;
          default:
            throw new RuntimeException("Invalid operator: " + operator);
        }
      }
    }

    // Update the rows
    update(toUpdate.toArray(new Entry[0]), columnIndexToUpdate, newEntryValue);
  }

  public void update(Entry[] primaryKeys, int columnIndexToUpdate, Entry newValue) {
    // TODO
    for (Entry primaryKey : primaryKeys) {
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

  private static class TableIterator implements Iterator<Row> {
    private final Iterator<Pair<Entry, Row>> iterator;

    public TableIterator(Table table) {
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
