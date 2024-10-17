package zipkin2.clickhouse.spanconsumer;

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

  final SchedulerSpanPersistence schedulerSpanPersistence;

  public ClickHouseSpanConsumer(SchedulerSpanPersistence schedulerSpanPersistence) {
    this.schedulerSpanPersistence = schedulerSpanPersistence;
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (CollectionUtils.isEmpty(spans)) {
      return Call.create(null);
    }
    SpansQueueManager.add(spans);
    //判断数据量进行保存
    schedulerSpanPersistence.runInsert(false);
    return null;
  }


}
