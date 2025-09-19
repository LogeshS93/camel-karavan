import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

@Configuration
@BindToRegistry("BuildRegelleistungTimeseries")
public class BuildRegelleistungTimeseries implements Processor {

    public void process(Exchange ex) throws Exception {
        String market = ex.getMessage().getHeader("market", String.class);
      if (market == null) market = "";

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rows = ex.getMessage().getBody(List.class);
      if (rows == null) rows = List.of();

      // Excel values often come as strings (with commas for decimal) — normalize later
      final ZoneId zone = ZoneId.of("Europe/Luxembourg");
      final DateTimeFormatter iso = DateTimeFormatter.ISO_INSTANT;
      final DateTimeFormatter dIso = DateTimeFormatter.ISO_LOCAL_DATE;      // yyyy-MM-dd
      final DateTimeFormatter dDMY = DateTimeFormatter.ofPattern("dd/MM/yyyy"); // e.g. 10/03/2025

      // Field mapping per market
      final boolean isFCR = "fcr".equalsIgnoreCase(market);
      final String productCol = isFCR ? "PRODUCTNAME" : "PRODUCT";
      final String valueCol   = isFCR
          ? "GERMANY_SETTLEMENTCAPACITY_PRICE_[EUR/MW]"
          : "GERMANY_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]";
      final String suffix = isFCR ? "_settlement_capacity" : "_marginal_capacity";

      Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();

      for (Map<String, Object> row : rows) {
        if (row == null || row.isEmpty()) continue;

        String dateRaw = str(row.get("DATE_FROM"));
        if (dateRaw == null || dateRaw.isBlank()) continue;

        // Support either yyyy-MM-dd or dd/MM/yyyy, also allow "...T..." by trimming at 'T'
        String dateText = dateRaw.contains("T") ? dateRaw.substring(0, dateRaw.indexOf('T')) : dateRaw;
        LocalDate date;
        try { date = LocalDate.parse(dateText, dIso); }
        catch (Exception e) { date = LocalDate.parse(dateText, dDMY); }

        String product = str(row.get(productCol));
        if (product == null) continue;

        // name: "de_{market}_{firstTokenLower}" + suffix
        String firstTok = product;
        int us = product.indexOf('_');
        if (us > 0) firstTok = product.substring(0, us);
        String name = "de_" + market.toLowerCase() + "_" + firstTok.toLowerCase() + suffix;

        // hour = token between first and second underscore, e.g. POS_12_16 => 12
        int hour = 0;
        String[] parts = product.split("_");
        if (parts.length >= 2) {
          try { hour = Integer.parseInt(parts[1]); } catch (Exception ignore) {}
        }

        // ts = local midnight + hour, then to UTC (DST handled by java.time)
        Instant ts = date.atStartOfDay(zone).plusHours(hour).toInstant();

        // value (empty -> null; commas -> dot)
        Double value = null;
        String vraw = str(row.get(valueCol));
        if (vraw != null && !vraw.isBlank()) {
          try { value = Double.valueOf(vraw.replace(",", ".")); } catch (Exception ignore) {}
        }

        Map<String, Object> point = new LinkedHashMap<>();
        point.put("ts", iso.format(ts));
        point.put("value", value);

        grouped.computeIfAbsent(name, k -> new ArrayList<>()).add(point);
      }

      // Emit: [{ name, values: [{ts, value}, ...] }, ...]
      List<Map<String, Object>> out = new ArrayList<>();
      for (var e : grouped.entrySet()) {
        Map<String, Object> series = new LinkedHashMap<>();
        series.put("name", e.getKey());
        series.put("values", e.getValue());
        out.add(series);
      }

      ex.getMessage().setHeader("country", "de");
      ex.getMessage().setBody(out);
        
    }
    private static String str(Object o) {
    return o == null ? null : o.toString().trim();
  }
}