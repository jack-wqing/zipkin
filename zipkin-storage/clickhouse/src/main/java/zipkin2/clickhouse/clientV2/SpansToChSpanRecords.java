package zipkin2.clickhouse.clientV2;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.clickhouse.Constants;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * @Author:liuwenqing
 * @Date:2024/10/18 19:41
 * @Description:
 **/
public class SpansToChSpanRecords {
  public static final Gson gson = new Gson();
  public static final String span(Span span) {
    if (Objects.isNull(span) || StringUtils.isBlank(span.id())) {
      return null;
    }
    CHSpanRecord record = new CHSpanRecord();
    record.setTraceId(strValue(span.traceId()));
    record.setParentId(strValue(span.parentId()));
    record.setId(strValue(span.id()));
    record.setKind(strValue(span.kind() == null ? null : span.kind().name()));
    record.setDuration(longValue(span.duration()));
    record.setName(strValue(span.name()));
    record.setTimestamp(longValue(span.timestamp()).toString());
    handleTag(span, record);
    record.setTime(DateFormatUtils.format(new Date(longValue(span.timestamp()) / 1000), Constants.DATE_FORMAT));
    record.setTagsSql("");
    return gson.toJson(record);
  }
  public static final void handleTag(Span span, CHSpanRecord record) {
    Map<String, String> tags = span.tags();
    record.setStatus(strValue(tags.get("status")));
    record.setTimestampMillis(strLongValue(tags.get("timestampMillis")));
    handleLocalEndpoint(span.localEndpoint(), record);
    handleRemoteEndpoint(span.remoteEndpoint(), record);
    record.setTagsRpcMethod(strValue(tags.get("rpc.method")));
    record.setTagsRpcService(strValue(tags.get("rpc.service")));
    record.setTagsAppname(strValue(tags.get("appname")));
    record.setTagsComponent(strValue(tags.get("component")));
    record.setTagsHttpUrl(strValue(tags.get("http.url")));
    record.setTagsHttpMethod(strHttpMethodValue(tags.get("http.method")));
    record.setTagsCatMessageId(strValue(tags.get("CatMessageId")));
    record.setTagsHttpPath( strValue(tags.get("http.path")));
    record.setTagsHttpRequestSize(strLongValue(tags.get("http.request.size")));
    record.setTagsHttpResponseSize(strLongValue(tags.get("http.response.size")));
    record.setTagsHttpStatusCode(strLongValue(tags.get("http.status.code")));
    record.setTagsLocalIpv4(strValue(tags.get("local.ipv4")));
    record.setTagsPeerIpv4(strValue(tags.get("peer.ipv4")));
    record.setTagsPeerService(strValue(tags.get("peer.service")));
    record.setTagsPeerPort(strLongValue(tags.get("peer.port")));
    record.setTagsPeerHostname(strValue(tags.get("peer.hostname")));
    record.setTagsPeerIpv6(strValue(tags.get("peer.ipv6")));
    record.setTagsProcessId(strLongValue(tags.get("process.id")));
    record.setTagsSpanKind(strValue(tags.get("span.kind")));
    record.setTagsWorkerId(strLongValue(tags.get("worker.id")));
    record.setLogFilePath(strValue(tags.get("log.file.path")));
  }
  public static final void handleLocalEndpoint(Endpoint endpoint, CHSpanRecord record) {
    endpoint = Objects.isNull(endpoint) ? Endpoint.newBuilder().build() : endpoint;
    record.setLocalEndpointIpv4(strValue(endpoint.ipv4()));
    record.setLocalEndpointServiceName(strValue(endpoint.serviceName()));
    record.setLocalEndpointPort(strValue(endpoint.port() == null ? "" : endpoint.port().toString()));
  }
  public static final void handleRemoteEndpoint(Endpoint endpoint, CHSpanRecord record) {
    endpoint = Objects.isNull(endpoint) ? Endpoint.newBuilder().build() : endpoint;
    record.setRemoteEndpointIpv4(strValue(endpoint.ipv4()));
    record.setRemoteEndpointServiceName(strValue(endpoint.serviceName()));
    record.setRemoteEndpointPort(strValue(endpoint.port() == null ? "" : endpoint.port().toString()));
  }
  public static final String strValue(String value) {
    if (Objects.isNull(value)) {
      return  "";
    }
    return value;
  }

  public static final Long longValue(Long value) {
    if (Objects.isNull(value)) {
      return Long.valueOf(0);
    }
    return value;
  }

  public static final Long strLongValue(String value) {
    if (Objects.isNull(value)) {
      return Long.valueOf(0);
    }
    return Long.valueOf(value);
  }

  public static final String strHttpMethodValue(String value) {
    if (Objects.isNull(value)) {
      return  "";
    }
    return value.replaceAll("'", "").replaceAll(" ", "")
      .replaceAll("\\n", "").replaceAll("\"", "");
  }

}
