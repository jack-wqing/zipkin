package zipkin2.clickhouse.spanconsumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.clickhouse.Constants;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * @Author:liuwenqing
 * @Date:2024/10/14 14:58
 * @Description:
 **/
public class BatchSql {
  private String startSql;
  public BatchSql(String startSql) {
    this.startSql = startSql;
  }
  public String generate(Span[] data) {
    StringBuilder sqlValueSb = new StringBuilder();
    Arrays.stream(data).parallel().filter(Objects::nonNull).forEach(span -> sqlValueSb.append(Constants.COMMA + span(span)));
    if (sqlValueSb.length() > 0) {
      return startSql + sqlValueSb.substring(1);
    }
    return Constants.BLANK;
  }
  private String span(Span span) {
    if (Objects.isNull(span) || StringUtils.isBlank(span.id())) {
      return "";
    }
    StringBuilder valueSb = new StringBuilder(Constants.LEFT_BRACKET);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(span.traceId()) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(span.parentId()) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(span.id()) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(span.kind() == null ? null : span.kind().name()) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(longValue(span.duration()) + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(span.name()) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(longValue(span.timestamp()) + Constants.COMMA);
    handleTag(span, valueSb);
    valueSb.append(Constants.SINGLE_QUOTA +  DateFormatUtils.format(new Date(longValue(span.timestamp()) / 1000), Constants.DATE_FORMAT) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append("''");
    valueSb.append(Constants.RIGHT_BRACKET);
    return valueSb.toString();
  }
  private void handleTag(Span span, StringBuilder valueSb) {
    Map<String, String> tags = span.tags();
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("status")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(strLongValue(tags.get("timestampMillis")) + Constants.COMMA);
    handleLocalEndpoint(span.localEndpoint(), valueSb);
    handleLocalEndpoint(span.remoteEndpoint(), valueSb);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("rpc.method")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("rpc.service")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("appname")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("component")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("http.url")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strHttpMethodValue(tags.get("http.method")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("CatMessageId")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("http.path")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(strLongValue(tags.get("http.request.size")) + Constants.COMMA);
    valueSb.append(strLongValue(tags.get("http.response.size")) + Constants.COMMA);
    valueSb.append(strLongValue(tags.get("http.status.code")) + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("local.ipv4")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("peer.ipv4")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("peer.service")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(strLongValue(tags.get("peer.port")) + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("peer.hostname")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("peer.ipv6")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(strLongValue(tags.get("process.id")) + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("span.kind")) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(strLongValue(tags.get("worker.id")) + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(tags.get("log.file.path")) + Constants.SINGLE_QUOTA + Constants.COMMA);
  }
  private void handleLocalEndpoint(Endpoint endpoint, StringBuilder valueSb) {
    endpoint = Objects.isNull(endpoint) ? Endpoint.newBuilder().build() : endpoint;
    valueSb.append(Constants.SINGLE_QUOTA + strValue(endpoint.ipv4()) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(endpoint.serviceName()) + Constants.SINGLE_QUOTA + Constants.COMMA);
    valueSb.append(Constants.SINGLE_QUOTA + strValue(endpoint.port() == null ? "" : endpoint.port().toString()) + Constants.SINGLE_QUOTA + Constants.COMMA);
  }

  private String strValue(String value) {
    if (Objects.isNull(value)) {
      return  "";
    }
    return value;
  }

  private Long longValue(Long value) {
    if (Objects.isNull(value)) {
      return Long.valueOf(0);
    }
    return value;
  }

  private Long strLongValue(String value) {
    if (Objects.isNull(value)) {
      return Long.valueOf(0);
    }
    return Long.valueOf(value);
  }

  private Integer integerValue(Integer value) {
    if (Objects.isNull(value)) {
      return Integer.valueOf(0);
    }
    return value;
  }

  private String strHttpMethodValue(String value) {

    if (Objects.isNull(value)) {
      return  "";
    }
    return value.replaceAll("'", "").replaceAll(" ", "")
        .replaceAll("\\n", "").replaceAll("\"", "");
  }

}
