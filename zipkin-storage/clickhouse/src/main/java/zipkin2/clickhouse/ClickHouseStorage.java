package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.clickhouse.spanconsumer.ClickHouseSpanConsumer;
import zipkin2.clickhouse.spanconsumer.SchedulerSpanPersistence;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * click 实现得存储机制
 */
public class ClickHouseStorage extends StorageComponent {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {

    private boolean strictTraceId = true;
    private boolean searchEnabled = true;
    private ClickHouseDataSource dataSource;
    private Executor executor;
    List<String> autocompleteKeys = new ArrayList<>();

    private String spanTable;

    private String traceTable;

    private long namesLookback;
    public int batchSize;

    public int parallelWriteSize;

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
    public Builder dataSource(ClickHouseDataSource dataSource) {
      if (dataSource == null) {
        throw new NullPointerException("dataSource == null");
      }
      this.dataSource = dataSource;
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

    @Override
    public StorageComponent build() {
      return new ClickHouseStorage(this);
    }
  }

  final ClickHouseDataSource dataSource;
  final DataSourceCall.Factory dataSourceCallFactory;
  final boolean strictTraceId;
  final boolean searchEnabled;
  final List<String> autocompleteKeys;
  public String spanTable;

  public String traceTable;
  public long namesLookback;

  final SchedulerSpanPersistence schedulerSpanPersistence;

  public ClickHouseStorage(Builder builder) {
    dataSource = builder.dataSource;
    if (dataSource == null) {
      throw new NullPointerException("datasource == null");
    }
    Executor executor = builder.executor;
    if (executor == null) {
      throw new NullPointerException("executor == null");
    }
    dataSourceCallFactory = new DataSourceCall.Factory(dataSource, executor);
    strictTraceId = builder.strictTraceId;
    searchEnabled = builder.searchEnabled;
    autocompleteKeys = builder.autocompleteKeys;
    spanTable = builder.spanTable;
    traceTable = builder.traceTable;
    namesLookback = builder.namesLookback;
    schedulerSpanPersistence = new SchedulerSpanPersistence(dataSource, spanTable, builder.batchSize, builder.parallelWriteSize);
    schedulerSpanPersistence.start();
  }

  public ClickHouseDataSource dataSource() {
    return dataSource;
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
    return new ClickHouseSpanConsumer(schedulerSpanPersistence);
  }

  @Override
  public CheckResult check() {
    try (ClickHouseConnection connection = dataSource.getConnection();
      ClickHouseStatement statement = connection.createStatement()) {
      //执行一条SQL语句进行判断
      statement.executeQuery(String.format(Constants.CHECK_SQL, spanTable));
    } catch (SQLException e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  @Override
  public String toString() {
    return "ClickHouseStorage{" + "dataSource=" + dataSource + '}';
  }

  @Override
  public void close() throws IOException {

  }

  /** Visible for testing */
  void clear() {
    try (ClickHouseConnection connection = dataSource.getConnection();
      ClickHouseStatement statement = connection.createStatement()) {

    } catch (SQLException | RuntimeException e) {
      throw new AssertionError(e);
    }

  }

}
