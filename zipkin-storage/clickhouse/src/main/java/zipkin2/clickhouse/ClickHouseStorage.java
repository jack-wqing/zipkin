package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import zipkin2.CheckResult;
import zipkin2.clickhouse.clientV2.ClickHouseSpanConsumer;
import zipkin2.clickhouse.clientV2.SchedulerSpanPersistenceClientV2;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * click 实现得存储机制
 */
public class ClickHouseStorage extends StorageComponent {

  private static final Logger logger = Logger.getLogger(ClickHouseStorage.class.getName());

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {

    private boolean strictTraceId = true;
    private boolean searchEnabled = true;
    private Client client;
    private Executor executor;
    List<String> autocompleteKeys = new ArrayList<>();

    private String spanTable;

    private String traceTable;

    private long namesLookback;
    public int batchSize;

    public int parallelWriteSize;
    private int schedulingTime;

    @Override
    public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }
    @Override
    public Builder searchEnabled(boolean searchEnabled) {
      this.strictTraceId = searchEnabled;
      return this;
    }
    @Override
    public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) {
        throw new NullPointerException("keys == null");
      }
      this.autocompleteKeys = keys;
      return this;
    }
    public Builder client(Client client) {
      if (client == null) {
        throw new NullPointerException("dataSource == null");
      }
      this.client = client;
      return this;
    }
    public Builder executor(Executor executor) {
      if (executor == null) {
        throw new NullPointerException("executor == null");
      }
      this.executor = executor;
      return this;
    }
    public Builder spanTable(String spanTable) {
      this.spanTable = spanTable;
      return this;
    }

    public Builder traceTable(String traceTable) {
      this.traceTable = traceTable;
      return this;
    }

    public Builder namesLookback(long namesLookback) {
      this.namesLookback = namesLookback;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder parallelWriteSize(int parallelWriteSize) {
      this.parallelWriteSize = parallelWriteSize;
      return this;
    }

    public Builder schedulingTime(int schedulingTime) {
      this.schedulingTime = schedulingTime;
      return this;
    }

    @Override
    public StorageComponent build() {
      return new ClickHouseStorage(this);
    }
  }

  final Client client;
  final DataSourceCall.Factory dataSourceCallFactory;
  final boolean strictTraceId;
  final boolean searchEnabled;
  final List<String> autocompleteKeys;
  public String spanTable;
  public String traceTable;
  public long namesLookback;

  final SchedulerSpanPersistenceClientV2 schedulerSpanPersistenceClientV2;

  public ClickHouseStorage(Builder builder) {
    client = builder.client;
    if (client == null) {
      throw new NullPointerException("datasource == null");
    }
    Executor executor = builder.executor;
    if (executor == null) {
      throw new NullPointerException("executor == null");
    }
    dataSourceCallFactory = new DataSourceCall.Factory(client, executor);
    strictTraceId = builder.strictTraceId;
    searchEnabled = builder.searchEnabled;
    autocompleteKeys = builder.autocompleteKeys;
    spanTable = builder.spanTable;
    traceTable = builder.traceTable;
    namesLookback = builder.namesLookback;
    schedulerSpanPersistenceClientV2 = new SchedulerSpanPersistenceClientV2(builder.client, spanTable, builder.batchSize, builder.parallelWriteSize, builder.schedulingTime);
    schedulerSpanPersistenceClientV2.start();
  }

  public Client client() {
    return client;
  }
  @Override
  public SpanStore spanStore() {
    return new ClickHouseSpanStore(this);
  }
  @Override
  public Traces traces() {
    return (Traces)spanStore();
  }
  @Override
  public ServiceAndSpanNames serviceAndSpanNames() {
    return (ServiceAndSpanNames)spanStore();
  }
  @Override
  public AutocompleteTags autocompleteTags() {
    return new ClickHouseAutocompleteTags(this);
  }
  // 系统过载判断
  @Override
  public boolean isOverCapacity(Throwable e) {
    return super.isOverCapacity(e);
  }
  @Override
  public SpanConsumer spanConsumer() {
    return new ClickHouseSpanConsumer(schedulerSpanPersistenceClientV2);
  }

  @Override
  public CheckResult check() {
    try (QueryResponse response = client.query(String.format(Constants.CHECK_SQL, spanTable)).get(10, TimeUnit.SECONDS)) {
      //执行一条SQL语句进行判断
      ClickHouseBinaryFormatReader reader = Client.newBinaryFormatReader(response);
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse storage check error ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse storage check error InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse storage check error TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse storage check error Exception", e);
    }
    return CheckResult.OK;
  }

  @Override
  public String toString() {
    return "ClickHouseStorage{" + "client=" + client + '}';
  }

  @Override
  public void close() throws IOException {

  }

  /** 可以在写清空表的操作，测试使用 */
  void clear() {
    //

  }

}
