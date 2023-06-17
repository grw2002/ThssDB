package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.DataFileErrorException;
import cn.edu.thssdb.exception.NotNullException;
import cn.edu.thssdb.exception.TypeNotMatchException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.query.QueryTable2;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.PageRow;
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
  Database database;
  public String databaseName;
  private int primaryIndex;
  private final String tableName;
  // Pair<pageId, offset>
  public transient BPlusTree<Entry, PageRow> index;

  public String getTableName() {
    return tableName;
  }

  public Table(Database database, String tableName, List<Column> columns) {
    // TODO
    super(tableName, columns); // 调用父类构造函数
    //    System.out.println("Mark "+tableName+" "+columns.toString());
    this.database = database;
    this.databaseName = database.getName();
    this.tableName = tableName;
    this.index = new BPlusTree<>(databaseName, tableName);
    for (int i = 0; i < columns.size(); i++) {
      if (this.columns.get(i).isPrimary()) {
        primaryIndex = i;
      }
      this.columns.get(i).setTable(this);
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
  public void persist() {
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
  public void recover() {
    if (index.size() != 0) {
      return;
    }
    String fileName = this.tableName + ".data";
    Path loadPath = Paths.get(fileName);

    if (!Files.exists(loadPath)) {
      System.out.println("No such file:" + loadPath);
      return;
    }
    try {
      if (Files.size(loadPath) == 0) {
        System.out.println("Empty data file. No existing index.");
        this.index = new BPlusTree<>(databaseName, tableName); // 如果文件为空，初始化 index 为一个空的 BPlusTree
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
          this.index = (BPlusTree<Entry, PageRow>) fileContent;
          this.index.setDatabaseAndTableName(databaseName, tableName);
          System.out.println("loading...");
        } else {
          System.out.println("Invalid data file content. Expected BPlusTree<Entry, Row>.");
        }
      } else {
        System.out.println("Empty data file. Initializing index as an empty BPlusTree.");
        this.index = new BPlusTree<>(databaseName, tableName);
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

  public static Entry entryParse(SQLParser.LiteralValueContext literal, Column column) {
    boolean notNull = column.isNotNull();
    if (literal.K_NULL() != null) {
      if (notNull) throw new NotNullException(column.getName());
      return new Entry(null);
    }

    switch (column.getType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        if (literal.NUMERIC_LITERAL() == null) {
          throw new TypeNotMatchException(column.getName());
        }
        break;
      case STRING:
        if (literal.STRING_LITERAL() == null) {
          throw new TypeNotMatchException(column.getName());
        }
        break;
    }
    String value = literal.getText();
    switch (column.getType()) {
      case INT:
        return new Entry(Integer.valueOf(value));
      case LONG:
        return new Entry(Long.valueOf(value));
      case FLOAT:
        return new Entry(Float.valueOf(value));
      case DOUBLE:
        return new Entry(Double.valueOf(value));
      case STRING:
        return new Entry(value.substring(1, value.length() - 1));
      default:
        throw new RuntimeException("Unknown column type.");
    }
  }

  // util: get all rows
  public List<String> getAllRowsInfo() {
    List<String> allRows = new ArrayList<>();

    BPlusTreeIterator<Entry, PageRow> iter = index.iterator();
    while (iter.hasNext()) {
      Pair<Entry, PageRow> pair = iter.next();
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
        Iterator<Pair<Entry, PageRow>> iterator = this.index.iterator();

        while (iterator.hasNext()) {
          Pair<Entry, PageRow> pair = iterator.next();
          row = pair.right;
          row.dropEntry(columnIndex); // 增加一个新的Entry
        }
      }
    }
  }

  @Override
  // util: initTransientFields BPlusTree & Map
  public void initTransientFields() {
    this.index = new BPlusTree<>(databaseName, tableName);
    super.initTransientFields();
  }

  public void alterType(String columnName, String newColumnType) {
    boolean findFlag = false;
    int columnIndex = -1;
    boolean notNull = false;

    for (Column column : columns) {
      if (column.getName().equals(columnName)) {
        column.setType(newColumnType);
        notNull = column.isNotNull();
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
        Iterator<Pair<Entry, PageRow>> iterator = this.index.iterator();
        Entry oldEntry;
        Entry newEntry;
        boolean ifError = false;

        while (iterator.hasNext()) {
          Pair<Entry, PageRow> pair = iterator.next();
          row = pair.right;
          oldEntry = row.getEntries().get(columnIndex);
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

  @Override
  public void insert(Row row) {
    Entry primaryKey = row.getEntries().get(primaryIndex);
    if (!index.contains(primaryKey)) {
      index.put(row.getEntries().get(primaryIndex), new PageRow(row));
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
                      column.isNotNull()));
          if (column.isPrimary()) {
            Entry primaryKey = entries[i];
            // 检查主键是否已经存在
            if (index.contains(primaryKey)) {
              throw new RuntimeException("Duplicate primary key '" + primaryKey + "'");
            }
          }
          valueIndex++;
        } else {
          // The column is not specified, use a default value
          if (findColumnByName(columnName).isNotNull()) {
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
    BPlusTreeIterator<Entry, PageRow> iterator = index.iterator();
    while (iterator.hasNext()) {
      Pair<Entry, PageRow> pair = iterator.next();
      Row row = pair.right;

      // Get the value in the column for this row
      Entry columnValueEntry = row.getEntries().get(findColumnIndexByName(columnName));
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

  public static List<Entry> filterCondition(Table table, SQLParser.ConditionContext condition) {
    Iterator iter = table.iterator();
    List<Entry> result = new ArrayList<>();
    while (iter.hasNext()) {
      Row row = (Row) iter.next();
      if (QueryTable2.isConditionSatisfied(table, row, condition)) {
        result.add(row.getEntries().get(table.primaryIndex));
      }
    }
    return result;
  }

  public void updateWithConditions(
      String columnName,
      SQLParser.LiteralValueContext newValue,
      SQLParser.ConditionContext condition) {
    System.out.println(
        "updateWithConditions"
            + " "
            + columnName
            + " "
            + newValue.getText()
            + " "
            + condition.getText());
    Column column = findColumnByName(columnName);
    if (column == null) {
      throw new ColumnNotExistException(columnName);
    }

    // Parse new value to Entry according to the column type
    Entry newEntryValue = entryParse(newValue, column);
    List<Entry> toUpdate = filterCondition(this, condition);
    // Update the rows
    update(toUpdate.toArray(new Entry[0]), column.getIndex(), newEntryValue);
  }

  public void update(Entry[] primaryKeys, int columnIndexToUpdate, Entry newValue) {
    // TODO
    for (Entry primaryKey : primaryKeys) {
      index.get(primaryKey).updateEntry(columnIndexToUpdate, newValue);
    }
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Row> {
    private final Iterator<Pair<Entry, PageRow>> iterator;

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
