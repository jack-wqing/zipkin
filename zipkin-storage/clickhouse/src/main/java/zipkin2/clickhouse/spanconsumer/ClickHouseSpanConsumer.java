package zipkin2.clickhouse.spanconsumer;

import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

import java.util.List;

/**
 * 对Span consumer进行消耗
 */

public class ClickHouseSpanConsumer implements SpanConsumer {
  @Override
  public Call<Void> accept(List<Span> spans) {
    return null;
  }


}
