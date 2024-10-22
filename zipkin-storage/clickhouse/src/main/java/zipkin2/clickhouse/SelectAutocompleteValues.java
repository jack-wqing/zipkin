package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelectAutocompleteValues implements Function<Client, List<String>> {

  private static final Logger logger = Logger.getLogger(SelectAutocompleteValues.class.getName());
  final String spanTable;
  final String term;
  final long namesLookback;

  public SelectAutocompleteValues(String spanTable, String term, long namesLookback) {
    this.spanTable = spanTable;
    this.term = term;
    this.namesLookback = namesLookback;
  }

  @Override
  public List<String> apply(Client client) {
    if (StringUtils.isBlank(term)) {
      return Collections.emptyList();
    }
    long endMillis = System.currentTimeMillis();
    long beginMillis = endMillis - namesLookback;
    Set<String> tagValues = Sets.newHashSet();
    String sql = String.format(Constants.TAG_VALUE_SQL, term, spanTable,
      DateFormatUtils.format(new Date(beginMillis), Constants.DATE_FORMAT),
      DateFormatUtils.format(new Date(endMillis), Constants.DATE_FORMAT));
    try (QueryResponse response = client.query(sql).get(10, TimeUnit.SECONDS)) {
      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
      while (reader.hasNext()) {
        reader.next();
        String termValue = reader.getString(term);
        if (StringUtils.isNotBlank(termValue)) {
          tagValues.add(termValue);
        }
      }
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse SelectAutocompleteValues ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse SelectAutocompleteValues InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse SelectAutocompleteValues TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse SelectAutocompleteValues Exception", e);
    }
    return Lists.newArrayList(tagValues);
  }
}
