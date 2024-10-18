package zipkin2.clickhouse.spanconsumer;

import com.clickhouse.jdbc.ClickHouseDataSource;
import zipkin2.Span;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @Author:liuwenqing
 * @Date:2024/10/14 20:24
 * @Description:
 **/
public class SchedulerSpanPersistence {

  public static final Logger logger = Logger.getLogger(SchedulerSpanPersistence.class.getName());

  private static final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

  private static ExecutorService executorService = null;

  final ClickHouseDataSource dataSource;
  final String spanTable;
  final int batchSize;

  public SchedulerSpanPersistence(ClickHouseDataSource dataSource, String spanTable, int batchSize, int parallelWriteSize) {
    this.dataSource = dataSource;
    this.spanTable = spanTable;
    this.batchSize = batchSize;
    if (parallelWriteSize > 1) {
      executorService = Executors.newFixedThreadPool(parallelWriteSize);
    }
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
  }

  private void doInsert(int size) {
    if (size == 0) {
      return;
    }
    logger.info("current size:" + size);
    Span[] spans = new Span[size];
    SpansQueueManager.partSpans(spans);
    ExecuteWriteExecutor executor = new ExecuteWriteExecutor(dataSource, spanTable, spans);
    if (Objects.isNull(executorService)) {
      executor.execute();
    } else {
      executorService.execute(() -> executor.execute());
    }
    SpansQueueManager.lastWriteMills.set(System.currentTimeMillis());
  }

}
