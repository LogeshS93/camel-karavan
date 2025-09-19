package com.enovos;

import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.poi.ss.usermodel.*;
import java.io.InputStream;
import java.util.*;

public class XlsxToSheet001 {

  @BindToRegistry("xlsxToSheet001")   // registers bean for Kamelet/Main
  public Processor processor() {
    return new Processor() {
      @Override public void process(Exchange ex) throws Exception {
        InputStream is = ex.getIn().getBody(InputStream.class);
        try (Workbook wb = WorkbookFactory.create(is)) {
          Sheet sheet = wb.getSheet("001");
          List<Map<String,Object>> rows = new ArrayList<>();
          if (sheet == null) { ex.getMessage().setBody(rows); return; }

          Iterator<Row> it = sheet.iterator();
          if (!it.hasNext()) { ex.getMessage().setBody(rows); return; }

          // header
          Row headerRow = it.next();
          List<String> headers = new ArrayList<>();
          for (Cell c : headerRow) {
            c.setCellType(CellType.STRING);
            headers.add(c.getStringCellValue());
          }
          // data
          while (it.hasNext()) {
            Row r = it.next();
            Map<String,Object> row = new LinkedHashMap<>();
            for (int i = 0; i < headers.size(); i++) {
              Cell c = r.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
              row.put(headers.get(i), readCell(c));
            }
            rows.add(row);
          }
          ex.getMessage().setBody(rows);
        }
      }
      private Object readCell(Cell c) {
        if (c == null) return null;
        return switch (c.getCellType()) {
          case STRING -> c.getStringCellValue();
          case BOOLEAN -> c.getBooleanCellValue();
          case NUMERIC -> DateUtil.isCellDateFormatted(c) ? c.getDateCellValue() : c.getNumericCellValue();
          case FORMULA -> {
            switch (c.getCachedFormulaResultType()) {
              case STRING -> yield c.getStringCellValue();
              case BOOLEAN -> yield c.getBooleanCellValue();
              case NUMERIC -> yield DateUtil.isCellDateFormatted(c) ? c.getDateCellValue() : c.getNumericCellValue();
              default -> yield null;
            }
          }
          default -> null;
        };
      }
    };
  }
}
