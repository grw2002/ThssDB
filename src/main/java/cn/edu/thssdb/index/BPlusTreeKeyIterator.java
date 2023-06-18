package cn.edu.thssdb.index;

import cn.edu.thssdb.storage.Cloneable;

import java.util.Iterator;
import java.util.LinkedList;

public class BPlusTreeKeyIterator<K extends Comparable<K>, V extends Cloneable<V>>
    implements Iterator<K> {
  private final LinkedList<BPlusTreeNode<K, V>> queue;
  private final LinkedList<K> buffer;

  BPlusTreeKeyIterator(BPlusTree<K, V> tree) {
    queue = new LinkedList<>();
    buffer = new LinkedList<>();
    if (tree.size() == 0) return;
    queue.add(tree.root);
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty() || !buffer.isEmpty();
  }

  @Override
  public K next() {
    if (buffer.isEmpty()) {
      while (true) {
        BPlusTreeNode<K, V> node = queue.poll();
        if (node instanceof BPlusTreeLeafNode) {
          for (int i = 0; i < node.size(); i++) buffer.add(node.keys.get(i));
          break;
        } else if (node instanceof BPlusTreeInternalNode)
          for (int i = 0; i <= node.size(); i++)
            queue.add(((BPlusTreeInternalNode<K, V>) node).children.get(i));
      }
    }
    return buffer.poll();
  }
}
