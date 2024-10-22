package zipkin2.clickhouse.clientV2;

import org.apache.commons.collections4.CollectionUtils;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

import java.util.List;

/**
 * 对Span consumer进行消耗
 *   对于批操作的处理
 */

public class ClickHouseSpanConsumer implements SpanConsumer {
  final SchedulerSpanPersistenceClientV2 schedulerSpanPersistenceClientV2;
  public ClickHouseSpanConsumer(SchedulerSpanPersistenceClientV2 schedulerSpanPersistenceClientV2) {
    this.schedulerSpanPersistenceClientV2 = schedulerSpanPersistenceClientV2;
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (CollectionUtils.isEmpty(spans)) {
      return Call.create(null);
    }
    SpansQueueManager.add(spans);
    return null;
  }


}
