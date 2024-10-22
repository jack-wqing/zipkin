package zipkin2.clickhouse.clientV2;

import zipkin2.Span;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ch写队列管理，控制批量写操作
 *  有ch得写入性能；
 */
public class SpansQueueManager {

  public static final Logger logger = Logger.getLogger(SpansQueueManager.class.getName());
  private static volatile LinkedBlockingQueue<Span> queue = new LinkedBlockingQueue<>();

  public static int size() {
    return queue.size();
  }
  public static final void add(List<Span> spans) {
    spans.forEach(queue::offer);
  }

  public static final Span poll() {
      try {
          return queue.poll(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.log(Level.WARNING, "SpansQueueManager poll InterruptedException", e);
      }
      return null;
  }

}
