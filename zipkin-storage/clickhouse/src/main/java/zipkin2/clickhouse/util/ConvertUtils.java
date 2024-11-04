package zipkin2.clickhouse.util;

import org.apache.commons.lang3.StringUtils;
import zipkin2.clickhouse.Constants;

import java.util.Map;
import java.util.Objects;

/**
 * @Author:liuwenqing
 * @Date:2024/11/1 17:19
 * @Description:
 **/
public class ConvertUtils {

  public static final String strValue(String value) {
    if (Objects.isNull(value)) {
      return Constants.BLANK;
    }
    return value;
  }

  public static final Long longValue(Long value) {
    if (Objects.isNull(value)) {
      return Long.valueOf(0);
    }
    return value;
  }

  public static final String strHttpMethodValue(String value) {
    if (Objects.isNull(value)) {
      return Constants.BLANK;
    }
    return value.replaceAll(Constants.SINGLE_QUOTA, Constants.BLANK).replaceAll(Constants.BLANK_CHAR, Constants.BLANK)
      .replaceAll("\\n", Constants.BLANK).replaceAll("\"", Constants.BLANK);
  }

  public static final Integer strToInteger(String value) {
    if (StringUtils.isBlank(value)) {
      return null;
    }
    return Integer.valueOf(value);
  }
  public static final Long strToLong(String value) {
    if (StringUtils.isBlank(value)) {
      return 0L;
    }
    return Long.valueOf(value);
  }

  /**
   * {ipv4=10.100.119.213, port=20011, ipv6=, serviceName=basic-server}
   * @param value
   * @return
   */
  private static Map<String, String> strToMap(String value) {

    return null;

  }

}
