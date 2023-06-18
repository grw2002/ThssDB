package cn.edu.thssdb.index;

import cn.edu.thssdb.storage.Cloneable;
import cn.edu.thssdb.utils.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class BPlusTree<K extends Comparable<K>, V extends Cloneable<V>>
    implements Iterable<Pair<K, V>>, Serializable {

  BPlusTreeNode<K, V> root;
  private int size;
  private transient String databaseName, tableName;

  private String getIdentifier() {
    return databaseName + "_" + tableName;
  }

  public BPlusTree(String databaseName, String tableName) {
    setDatabaseAndTableName(databaseName, tableName);
    root = new BPlusTreeLeafNode<>(0, getIdentifier());
  }

  public void setDatabaseAndTableName(String databaseName, String tableName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  public int size() {
    return size;
  }

  public V get(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to get() is null");
    return root.get(key);
  }

  public void update(K key, V value) {
    root.remove(key, getIdentifier());
    root.put(key, value, getIdentifier());
  }

  public void put(K key, V value) {
    if (key == null) throw new IllegalArgumentException("argument key to put() is null");
    root.put(key, value, getIdentifier());
    size++;
    checkRoot();
  }

  public void remove(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to remove() is null");
    root.remove(key, getIdentifier());
    size--;
    if (root instanceof BPlusTreeInternalNode && root.size() == 0) {
      root = ((BPlusTreeInternalNode<K, V>) root).children.get(0);
    }
  }

  public boolean contains(K key) {
    if (key == null) throw new IllegalArgumentException("argument key to contains() is null");
    return root.containsKey(key);
  }

  private void checkRoot() {
    if (root.isOverFlow()) {
      BPlusTreeNode<K, V> newSiblingNode = root.split(getIdentifier());
      BPlusTreeInternalNode<K, V> newRoot = new BPlusTreeInternalNode<>(1);
      newRoot.keys.set(0, newSiblingNode.getFirstLeafKey());
      newRoot.children.set(0, root);
      newRoot.children.set(1, newSiblingNode);
      root = newRoot;
    }
  }

  @Override
  public BPlusTreeIterator<K, V> iterator() {
    return new BPlusTreeIterator<>(this);
  }

  public BPlusTreeKeyIterator<K, V> keyIterator() {
    return new BPlusTreeKeyIterator<>(this);
  }

  public List<V> getValues(List<K> keys) {
    List<V> result = new ArrayList<>();
    Iterator<Pair<K, V>> iter = this.iterator();
    if (!iter.hasNext()) {
      return result;
    }
    Pair<K, V> item = iter.next();
    for (K key : keys) {
      while (item.left.compareTo(key) < 0 && iter.hasNext()) {
        item = iter.next();
      }
      if (item.left.compareTo(key) == 0) {
        result.add(item.right);
      }
      if (!iter.hasNext()) {
        break;
      }
    }
    return result;
  }
}
