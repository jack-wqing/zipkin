package zipkin2.clickhouse.spanconsumer;

import zipkin2.Span;

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

  public static final void partSpans(Span[] spans) {
    if (queue.isEmpty()) {
     return;
    }
    synchronized (lock) {
      for (int i = 0; i < spans.length; i++) {
        if (queue.isEmpty()) {
          break;
        }
        spans[i] = queue.pop();
      }
    }
  }

}
