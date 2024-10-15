package zipkin2.clickhouse.spanconsumer;

import zipkin2.Span;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ch写队列管理，控制批量写操作
 *  有ch得写入性能；
 */
public class SpansQueueManager {

  private static final LinkedList<Span> queue = new LinkedList<>();

  private static final Object lock = new Object();

  public static final AtomicLong lastWriteMills = new AtomicLong();

  public static int size() {
    return queue.size();
  }

  public static final synchronized void add(List<Span> spans) {
    if (Objects.isNull(spans)) {
      return;
    }
    synchronized (lock) {
      spans.forEach(queue::offerLast);
    }
  }

  public static final List<Span> partSpans(int size) {
    if (queue.isEmpty()) {
      return Collections.emptyList();
    }
    LinkedList<Span> result = new LinkedList<>();
    synchronized (lock) {
      for (int i = 0; i < size; i++) {
        if (queue.isEmpty()) {
          break;
        }
        result.add(queue.pop());
      }
    }
    return result;
  }

}
