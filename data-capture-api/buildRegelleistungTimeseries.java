import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import java.time.LocalDateTime;
import java.time.OffsetDateTime; 
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Configuration
@BindToRegistry("buildRegelleistungTimeseries")
public class buildRegelleistungTimeseries implements Processor {
    private static final Logger LOG = LoggerFactory.getLogger(buildRegelleistungTimeseries.class);

    public void process(Exchange ex) throws Exception {
       String market = ex.getMessage().getHeader("market", String.class);
      if (market == null) market = "";
      market = market.toLowerCase(Locale.ROOT);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rows = ex.getMessage().getBody(List.class);
      if (rows == null) rows = List.of();

      ZoneId zone = ZoneId.of("Europe/Luxembourg");
      DateTimeFormatter isoInstant = DateTimeFormatter.ISO_INSTANT;

      boolean isFCR = "fcr".equals(market);
      String productCol = isFCR ? "PRODUCTNAME" : "PRODUCT";
      String valueCol   = isFCR
          ? "GERMANY_SETTLEMENTCAPACITY_PRICE_[EUR/MW]"
          : "GERMANY_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]";
      String suffix = isFCR ? "_settlement_capacity" : "_marginal_capacity";

      Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();

      for (Map<String, Object> row : rows) {
        if (row == null || row.isEmpty()) continue;

        Object rawDate = row.get("DATE_FROM");
        LocalDate date = coerceToLocalDate(rawDate, zone);
        if (date == null) {
          LOG.warn("Skipping row: unparseable DATE_FROM='{}'", rawDate);
          continue;
        }

        String product = str(row.get(productCol));
        if (product == null) {
          LOG.warn("Skipping row: missing {}", productCol);
          continue;
        }

        // name = "de_{market}_{firstTokenLower}{suffix}"
        String firstTok = product;
        int us = product.indexOf('_');
        if (us > 0) firstTok = product.substring(0, us);
        String name = "de_" + market + "_" + firstTok.toLowerCase(Locale.ROOT) + suffix;

        int hour = extractHour(product); // token between first and second underscore
        Instant ts = date.atStartOfDay(zone).plusHours(hour).toInstant(); // DST-safe

        Double value = parseNumber(row.get(valueCol));

        Map<String, Object> point = new LinkedHashMap<>();
        point.put("ts", isoInstant.format(ts));  // UTC ISO string
        point.put("value", value);

        grouped.computeIfAbsent(name, k -> new ArrayList<>()).add(point);
      }

      List<Map<String, Object>> out = new ArrayList<>();
      for (Map.Entry<String, List<Map<String, Object>>> e : grouped.entrySet()) {
        Map<String, Object> series = new LinkedHashMap<>();
        series.put("name", e.getKey());
        series.put("values", e.getValue());
        out.add(series);
      }

      ex.getMessage().setHeader("country", "de");
      ex.getMessage().setBody(out);
        
    }
     private static LocalDate coerceToLocalDate(Object src, ZoneId zone) {
    if (src == null) return null;

    // If already a java.util.Date
    if (src instanceof java.util.Date d) {
      return d.toInstant().atZone(zone).toLocalDate();
    }

    // Excel date serial number (days since 1899-12-30)
    if (src instanceof Number n) {
      long days = n.longValue();
      return LocalDate.of(1899, 12, 30).plusDays(days);
    }

    String s = src.toString().trim();
    if (s.isEmpty()) return null;

    // Trim at 'T' to try pure date first
    String head = s.contains("T") ? s.substring(0, s.indexOf('T')) : s;

    // Try a bunch of common date-only formats
    String[] datePatterns = new String[] {
        "yyyy-MM-dd",
        "dd/MM/yyyy",
        "dd.MM.yyyy",
        "dd-MM-yyyy",
        "yyyy/MM/dd",
        "yyyyMMdd"
    };
    for (String p : datePatterns) {
      try {
        return DateTimeFormatter.ofPattern(p).parse(head, LocalDate::from);
      } catch (DateTimeParseException ignore) {}
    }

    // Try full datetime ISO/offset patterns
    try {
      // e.g. 2025-09-19T00:00:00Z or with offset
      return OffsetDateTime.parse(s).atZoneSameInstant(zone).toLocalDate();
    } catch (DateTimeParseException ignore) {}
    try {
      // e.g. 2025-09-19T00:00:00 or 2025-09-19T00:00
      return LocalDateTime.parse(s, DateTimeFormatter.ISO_LOCAL_DATE_TIME).atZone(zone).toLocalDate();
    } catch (DateTimeParseException ignore) {}

    return null;
  }

  private static int extractHour(String product) {
    String[] parts = product.split("_");
    if (parts.length >= 2) {
      try { return Integer.parseInt(parts[1]); } catch (Exception ignore) {}
    }
    return 0;
  }

  private static Double parseNumber(Object v) {
    if (v == null) return null;
    String s = v.toString().trim();
    if (s.isEmpty()) return null;
    try {
      // support "1,23" as well
      return Double.valueOf(s.replace(",", "."));
    } catch (Exception e) {
      return null;
    }
  }

  private static String str(Object o) {
    return o == null ? null : o.toString().trim();
  }
}