package zipkin2.clickhouse.spanconsumer;

import com.clickhouse.jdbc.ClickHouseDataSource;
import zipkin2.Span;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Author:liuwenqing
 * @Date:2024/10/14 20:24
 * @Description:
 **/
public class SchedulerSpanPersistence {

  public static final Logger logger = Logger.getLogger(SchedulerSpanPersistence.class.getName());

  private static final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

  private static final AtomicBoolean runInsert = new AtomicBoolean(false);

  final ClickHouseDataSource dataSource;
  final String spanTable;
  final int batchSize;

  public SchedulerSpanPersistence(ClickHouseDataSource dataSource, String spanTable, int batchSize) {
    this.dataSource = dataSource;
    this.spanTable = spanTable;
    this.batchSize = batchSize;
  }

  public void start(){
    logger.info("SchedulerSpanPersistence starting");
    scheduledExecutorService.scheduleWithFixedDelay(new BatchAddTask(), 1, 1, TimeUnit.SECONDS);
  }

  class BatchAddTask implements Runnable {
    @Override
    public void run() {
      long lastMills = SpansQueueManager.lastWriteMills.get();
      //表示已经距离上次小于1s,则本次不在执行
      if ((System.currentTimeMillis() - lastMills) < 1000) {
        return;
      }
      //如果剩余的数据量还是大于设置的batchSize数量，则在进行保存
      runInsert(true);
    }
  }

  public final void runInsert(boolean scheduling) {
    if (!setRunning()) {
      return;
    }
    try {
      if (scheduling) {
        int size = SpansQueueManager.size();
        if (size > batchSize) {
          size = batchSize;
        }
        doInsert(size);
      }
      while (SpansQueueManager.size() >= batchSize) {
        doInsert(batchSize);
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "batch save clickhouse error", e);
    } finally {
      setNoRunning();
    }
  }

  private void doInsert(int size) {
    if (size == 0) {
      return;
    }
    List<Span> spans = SpansQueueManager.partSpans(size);
    ExecuteWriteExecutor executor = new ExecuteWriteExecutor(dataSource, spanTable, spans);
    executor.execute();
    SpansQueueManager.lastWriteMills.set(System.currentTimeMillis());
  }


  private static boolean setRunning() {
    return runInsert.compareAndSet(false, true);
  }

  private static boolean setNoRunning() {
    return runInsert.compareAndSet(true, false);
  }


}
