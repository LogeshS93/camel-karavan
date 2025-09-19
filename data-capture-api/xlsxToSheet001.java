import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.poi.ss.usermodel.*;
import java.io.InputStream;
import java.util.*;

@Configuration
@BindToRegistry("xlsxToSheet001")
public class xlsxToSheet001 implements Processor {

    public void process(Exchange ex) throws Exception {
        InputStream is = ex.getIn().getBody(InputStream.class);
        try (Workbook wb = WorkbookFactory.create(is)) {
          Sheet sheet = wb.getSheet("001");
          if (sheet == null) sheet = wb.getSheetAt(0);   // fallback

          List<Map<String, Object>> out = new ArrayList<>();
          DataFormatter fmt = new DataFormatter(Locale.US);      // render as displayed (e.g., "1.41")
          FormulaEvaluator eval = wb.getCreationHelper().createFormulaEvaluator();

          int firstRow = sheet.getFirstRowNum();
          Row headerRow = sheet.getRow(firstRow);
          if (headerRow == null) { ex.getMessage().setBody(out); return; }

          // Build headers by column index (avoid missing cells/merges)
          int lastCol = headerRow.getLastCellNum(); // inclusive end is lastCol-1
          List<String> headers = new ArrayList<>(lastCol);
          for (int c = 0; c < lastCol; c++) {
            String h = fmt.formatCellValue(headerRow.getCell(c), eval).trim();
            if (h.isEmpty()) h = "COL_" + (c + 1);
            headers.add(h);
          }

          // Rows
          for (int r = firstRow + 1; r <= sheet.getLastRowNum(); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            for (int c = 0; c < headers.size(); c++) {
              String text = fmt.formatCellValue(row.getCell(c), eval);
              m.put(headers.get(c), text);  // keep strings exactly as shown in Excel
            }
            // (optional) skip completely empty rows
            if (m.values().stream().anyMatch(v -> v != null && !v.toString().isBlank())) {
              out.add(m);
            }
          }

          ex.getMessage().setBody(out);  // List<Map<String,Object>>
        }
}
}