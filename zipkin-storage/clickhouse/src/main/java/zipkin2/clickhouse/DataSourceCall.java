package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
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
    final Client client;
    final Executor executor;

    public Factory(Client client, Executor executor) {
      this.client = client;
      this.executor = executor;
    }
    <V> DataSourceCall<V> create(Function<Client, V> queryFunction) {
      return new DataSourceCall<>(this, queryFunction);
    }
  }
  final Factory factory;
  final Function<Client, V> queryFunction;
  public DataSourceCall(Factory factory, Function<Client, V> queryFunction) {
    this.factory = factory;
    this.queryFunction = queryFunction;
  }
  @Override
  protected V doExecute() throws IOException {
    return queryFunction.apply(factory.client);
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
