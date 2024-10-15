package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import zipkin2.Call;
import zipkin2.Callback;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * 表示clickhouse的操作
 */
public class DataSourceCall<V> extends Call.Base<V>{
  public static final class Factory {
    final ClickHouseDataSource dataSource;
    final Executor executor;

    public Factory(ClickHouseDataSource dataSource, Executor executor) {
      this.dataSource = dataSource;
      this.executor = executor;
    }
    <V> DataSourceCall<V> create(Function<ClickHouseConnection, V> queryFunction) {
      return new DataSourceCall<>(this, queryFunction);
    }
  }
  final Factory factory;
  final Function<ClickHouseConnection, V> queryFunction;
  public DataSourceCall(Factory factory, Function<ClickHouseConnection, V> queryFunction) {
    this.factory = factory;
    this.queryFunction = queryFunction;
  }
  @Override
  protected V doExecute() throws IOException {
    try (ClickHouseConnection connection = factory.dataSource.getConnection()) {
      return queryFunction.apply(connection);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
  @Override
  protected void doEnqueue(Callback<V> callback) {
    class CallbackRunnable implements Runnable {
      @Override
      public void run() {
        try {
          callback.onSuccess(doExecute());
        } catch (IOException e) {
          if (e.getCause() instanceof SQLException) {
            callback.onError(e.getCause());
          } else {
            callback.onError(e);
          }
        } catch (Throwable t) {
          propagateIfFatal(t);
          callback.onError(t);
        }
      }
    }
    factory.executor.execute(new CallbackRunnable());
  }
  @Override
  public Call<V> clone() {
    return new DataSourceCall<>(factory, queryFunction);
  }

}
