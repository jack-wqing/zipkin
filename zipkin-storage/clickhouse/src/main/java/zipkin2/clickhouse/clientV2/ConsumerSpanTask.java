package zipkin2.clickhouse.clientV2;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.commons.lang3.StringUtils;
import zipkin2.Span;

import java.io.ByteArrayInputStream;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Author:liuwenqing
 * @Date:2024/10/21 19:33
 * @Description:
 **/
public class ConsumerSpanTask implements Runnable{

  private static final Logger logger = Logger.getLogger(ConsumerSpanTask.class.getName());

  final Client client;
  final String spanTable;
  final int batchSize;
  final int schedulingTime;
  private volatile long lastSaveMills = System.currentTimeMillis();
  private volatile StringBuilder data = new StringBuilder();
  private volatile AtomicInteger count =  new AtomicInteger(0);

  public ConsumerSpanTask(Client client, String spanTable, int batchSize, int schedulingTime) {
    this.client = client;
    this.spanTable = spanTable;
    this.batchSize = batchSize;
    this.schedulingTime = schedulingTime * 1000;
  }

  @Override
  public void run() {
    try {
      //通过队列数据，直接拼接sql,优化空间
      while (true) {
        //依赖阻塞队列的1s等待，避免空循环占资源
        Span span = SpansQueueManager.poll();
        if (Objects.isNull(span)) {
          continue;
        }
        String dataStr = SpansToChSpanRecords.span(span);
        if (StringUtils.isBlank(dataStr)) {
          continue;
        }
        data.append(dataStr);
        int curCount = count.incrementAndGet();
        if ((System.currentTimeMillis() - lastSaveMills) > schedulingTime || curCount >= batchSize) {
          doSave(curCount, data.toString());
          reset();
          continue;
        }
        data.append(System.lineSeparator());
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "ConsumerSpanTask run error!", e);
    }

  }

  private void doSave(int curSize, String data) {
    InsertSettings insertSettings = new InsertSettings();
    try (InsertResponse insertResponse = client.insert(spanTable, new ByteArrayInputStream(data.getBytes()),
      ClickHouseFormat.JSONEachRow, insertSettings).get(10, TimeUnit.SECONDS)) {
      logger.info("curSize:" + curSize + ",success row:" + insertResponse.getResultRows());
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse ExecuteWriteExecutorClientV2 batch insert ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse ExecuteWriteExecutorClientV2 batch insert InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse ExecuteWriteExecutorClientV2 batch insert TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse ExecuteWriteExecutorClientV2 batch insert Exception", e);
    }
  }

  private void reset() {
    count.set(0);
    lastSaveMills = System.currentTimeMillis();
    data.delete(0, data.length());
  }

}
