package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.google.common.collect.Lists;
import zipkin2.Span;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelectSpansByTraceId implements Function<Client, List<Span>> {
  private static final Logger logger = Logger.getLogger(SelectSpansByTraceId.class.getName());
  private String traceId;
  private String traceTable;

  public SelectSpansByTraceId(String traceTable, String traceId) {
    this.traceTable = traceTable;
    this.traceId = traceId;
  }
  @Override
  public List<Span> apply(Client client) {
    List<Span> result = Lists.newArrayList();
    try (QueryResponse response = client.query(String.format(Constants.SPAN_TRACE_ID_SQL, traceTable, traceId)).get(10, TimeUnit.SECONDS)) {
      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
      result = ResultSetToSpanHelper.resultSetToSpan(reader);
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceId ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceId InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceId TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceId Exception", e);
    }
    return result;
  }
}
