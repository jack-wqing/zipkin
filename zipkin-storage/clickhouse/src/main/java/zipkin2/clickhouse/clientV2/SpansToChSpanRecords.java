package zipkin2.clickhouse.clientV2;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.clickhouse.Constants;
import zipkin2.clickhouse.util.ConvertUtils;
import zipkin2.clickhouse.util.JSONUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Author:liuwenqing
 * @Date:2024/10/18 19:41
 * @Description:
 **/
public class SpansToChSpanRecords {
  public static final String span(Span span) {
    if (Objects.isNull(span) || StringUtils.isBlank(span.id())) {
      return null;
    }
    CHSpanRecord record = new CHSpanRecord();
    record.setTraceId(ConvertUtils.strValue(span.traceId()));
    record.setParentId(ConvertUtils.strValue(span.parentId()));
    record.setId(ConvertUtils.strValue(span.id()));
    record.setKind(span.kind() == null ? Constants.BLANK : span.kind().name());
    record.setName(ConvertUtils.strValue(span.name()));
    record.setDuration(ConvertUtils.longValue(span.duration()));
    record.setTimestamp(ConvertUtils.longValue(span.timestamp()));
    record.setTimestampMillis(record.getTimestamp() / 1000);
    record.setTime(DateFormatUtils.format(new Date(record.getTimestampMillis()), Constants.DATE_FORMAT));
    handleEndpoint(span.localEndpoint(), record, true);
    handleEndpoint(span.remoteEndpoint(), record, false);
    handleAnnotations(span.annotations(), record);
    handleTag(span, record);
    return JSONUtils.toJSONString(record);
  }
  public static final void handleTag(Span span, CHSpanRecord record) {
    Map<String, String> tags = span.tags();
    if (MapUtils.isEmpty(tags)) {
      return;
    }
    if (StringUtils.isNotBlank(tags.get(Constants.HTTP_METHOD))) {
      tags.put(Constants.HTTP_METHOD, ConvertUtils.strHttpMethodValue(Constants.HTTP_METHOD));
    }
    if (StringUtils.isBlank(tags.get(Constants.TAG_ERROR))) {
      tags.put(Constants.TAG_ERROR_STATUS, Constants.TAG_ERROR_STATUS_OK);
    } else {
      tags.put(Constants.TAG_ERROR_STATUS, Constants.TAG_ERROR_STATUS_ERROR);
    }
    //手动透明设置错误标识，方便统计操作
    record.setTags(tags);
  }
  public static final void handleEndpoint(Endpoint endpoint, CHSpanRecord record, boolean local) {
    if (Objects.isNull(endpoint)) {
      return;
    }
    String serviceName = ConvertUtils.strValue(endpoint.serviceName());
    if (local) {
      record.setServiceName(serviceName);
    }
    record.addEndpoint(Constants.ENDPOINT_SERVICE_NAME, serviceName, local);
    record.addEndpoint(Constants.ENDPOINT_IPV4, ConvertUtils.strValue(endpoint.ipv4()), local);
    record.addEndpoint(Constants.ENDPOINT_IPV6, ConvertUtils.strValue(endpoint.ipv6()), local);
    record.addEndpoint(Constants.ENDPOINT_PORT, endpoint.port() == null ? Constants.BLANK : endpoint.port().toString(), local);
  }

  public static final void handleAnnotations(List<Annotation> annotationList, CHSpanRecord record) {
    if (CollectionUtils.isEmpty(annotationList)) {
      return;
    }
    for (Annotation annotation : annotationList) {
      if (annotation == null || StringUtils.isBlank(annotation.value())) {
        continue;
      }
      record.addAnnotation(annotation.value(), String.valueOf(annotation.timestamp()));
    }
  }

}
