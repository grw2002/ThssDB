package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null) return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries) sj.add(e.toString());
    return sj.toString();
  }

  public Row combine(Row poll) {
    Entry[] entries = new Entry[this.entries.size() + poll.entries.size()];
    for (int i = 0; i < this.entries.size(); i++) {
      entries[i] = this.entries.get(i);
    }
    for (int i = 0; i < poll.entries.size(); i++) {
      entries[i + this.entries.size()] = poll.entries.get(i);
    }
    return new Row(entries);
  }
}
