package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * clickhouse: 通过时间窗口控制服务名
 */
public class SelectServiceSpanName implements Function<Client, List<String>> {

  private static final Logger logger = Logger.getLogger(SelectServiceSpanName.class.getName());

  private final String spanTable;
  private long namesLookback;
  private String serviceName;
  private String term;

  public SelectServiceSpanName(String spanTable, long namesLookback, String serviceName, String term) {
    this.spanTable = spanTable;
    this.namesLookback = namesLookback;
    this.serviceName = serviceName;
    this.term = term;
  }

  @Override
  public List<String> apply(Client client) {
    long endMillis = System.currentTimeMillis();
    long beginMillis = endMillis - namesLookback;
    Set<String> serviceNames = Sets.newHashSet();
    String sql = String.format(Constants.SERVICE_TERM_SQL, term, spanTable, serviceName,
      DateFormatUtils.format(new Date(beginMillis), Constants.DATE_FORMAT),
      DateFormatUtils.format(new Date(endMillis), Constants.DATE_FORMAT));
    try (QueryResponse response = client.query(sql).get(10, TimeUnit.SECONDS)) {
      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
      while (reader.hasNext()) {
        reader.next();
        String serviceName = reader.getString(term);
        if (StringUtils.isNotBlank(serviceName)) {
          serviceNames.add(serviceName);
        }
      }
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse SelectServiceSpanName ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse SelectServiceSpanName InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse SelectServiceSpanName TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse SelectServiceSpanName Exception", e);
    }
    return Lists.newArrayList(serviceNames);

  }
}
