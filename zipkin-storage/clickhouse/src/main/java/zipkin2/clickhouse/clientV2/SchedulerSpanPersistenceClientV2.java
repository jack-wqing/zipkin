package zipkin2.clickhouse.clientV2;

import com.clickhouse.client.api.Client;
import zipkin2.clickhouse.Constants;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * @Author:liuwenqing
 * @Date:2024/10/14 20:24
 * @Description:
 **/
public class SchedulerSpanPersistenceClientV2 {

  public static final Logger logger = Logger.getLogger(SchedulerSpanPersistenceClientV2.class.getName());
  // 并行写入
  private static ExecutorService executorService = null;
  final Client client;
  final String spanTable;
  final int batchSize;
  final int schedulingTime;
  final int parallelWriteSize;
  public SchedulerSpanPersistenceClientV2(Client client, String spanTable, int batchSize, int parallelWriteSize, int schedulingTime) {
    this.client = client;
    this.spanTable = spanTable;
    this.batchSize = batchSize;
    this.schedulingTime = schedulingTime;
    this.parallelWriteSize = parallelWriteSize;
    DaemonThreadFactory daemonThreadFactory = new DaemonThreadFactory(Constants.BATCH_INSERT_THREAD_POOL_NAME);
    executorService = parallelWriteSize == 1 ? Executors.newSingleThreadExecutor(daemonThreadFactory) :
      Executors.newFixedThreadPool(parallelWriteSize, daemonThreadFactory);
  }

  public void start(){
    logger.info("SchedulerSpanPersistence starting");
    for (int i = 0; i < parallelWriteSize; i++) {
      executorService.execute(new ConsumerSpanTask(client, spanTable, batchSize, schedulingTime));
    }
  }

  class DaemonThreadFactory implements ThreadFactory{
    private AtomicInteger threadNo = new AtomicInteger(1);
    private final String nameStart;
    private final String nameEnd = "]";
    public DaemonThreadFactory(String poolName) {
      this.nameStart = "[" + poolName + "-";
    }
    public Thread newThread(Runnable r) {
      String threadName = this.nameStart + this.threadNo.getAndIncrement() + nameEnd;
      Thread newThread = new Thread(r, threadName);
      newThread.setDaemon(true);
      if (newThread.getPriority() != 5) {
        newThread.setPriority(5);
      }
      return newThread;
    }
  }

}
