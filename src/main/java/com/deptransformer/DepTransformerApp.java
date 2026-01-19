package com.deptransformer;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DepTransformerApp {
  private static final Charset DBF_CHARSET = StandardCharsets.ISO_8859_1;
  private static final DateTimeFormatter BACKUP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
  private static final List<String> VALUE_COLUMNS = List.of(
      "IGe",
      "DIPP",
      "DPE365",
      "DPE450",
      "DPN",
      "MP120",
      "MP210",
      "DP120",
      "DP210",
      "DP365",
      "DP450",
      "DPAC"
  );

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      printUsage();
      return;
    }

    String command = args[0].toLowerCase(Locale.ROOT);
    Map<String, String> options = parseOptions(args);

    switch (command) {
      case "list-dbf" -> listDbf(
          getRequiredOption(options, "--file"),
          parseIntOption(options, "--limit", 100),
          parseIntOption(options, "--offset", 0)
      );
      case "clear-dbf" -> clearDbf(getRequiredOption(options, "--file"));
      case "import-csv" -> importCsv(
          getRequiredOption(options, "--csv"),
          getRequiredOption(options, "--dep"),
          getRequiredOption(options, "--descdep")
      );
      default -> {
        System.err.println("Comando desconhecido: " + command);
        printUsage();
      }
    }
  }

  private static void printUsage() {
    System.out.println("Uso:");
    System.out.println("  list-dbf --file <caminho> [--limit 100] [--offset 0]");
    System.out.println("  clear-dbf --file <caminho>");
    System.out.println("  import-csv --csv <arquivo.csv> --dep <DEP.DBF> --descdep <descdep.dbf>");
  }

  private static void listDbf(String filePath, int limit, int offset) throws IOException {
    Path path = Path.of(filePath);
    try (InputStream input = new BufferedInputStream(Files.newInputStream(path))) {
      DBFReader reader = new DBFReader(input, DBF_CHARSET);
      int fieldCount = reader.getFieldCount();
      List<String> fieldNames = new ArrayList<>();
      for (int i = 0; i < fieldCount; i++) {
        fieldNames.add(reader.getField(i).getName());
      }
      System.out.println("Colunas: " + String.join(", ", fieldNames));

      int index = 0;
      int printed = 0;
      Object[] row;
      while ((row = reader.nextRecord()) != null) {
        if (index++ < offset) {
          continue;
        }
        if (printed++ >= limit) {
          break;
        }
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
          if (i > 0) {
            line.append(" | ");
          }
          line.append(fieldNames.get(i)).append("=").append(row[i]);
        }
        System.out.println(line);
      }
    }
  }

  private static void clearDbf(String filePath) throws IOException {
    Path path = Path.of(filePath);
    if (!Files.exists(path)) {
      throw new IllegalArgumentException("Arquivo não encontrado: " + filePath);
    }

    Path backup = createBackup(path);
    System.out.println("Backup criado em: " + backup);

    DBFField[] fields;
    try (InputStream input = new BufferedInputStream(Files.newInputStream(path))) {
      DBFReader reader = new DBFReader(input, DBF_CHARSET);
      fields = new DBFField[reader.getFieldCount()];
      for (int i = 0; i < fields.length; i++) {
        fields[i] = reader.getField(i);
      }
    }

    try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(path))) {
      DBFWriter writer = new DBFWriter(output, DBF_CHARSET);
      writer.setFields(fields);
      writer.close();
    }
  }

  private static void importCsv(String csvPath, String depPath, String descdepPath) throws IOException {
    Path csv = Path.of(csvPath);
    Path dep = Path.of(depPath);
    Path descdep = Path.of(descdepPath);

    List<CsvColumn> columns = buildCsvColumns();
    DescdepResult descdepResult = ensureDescdep(descdep, columns);

    DBFField[] depFields;
    List<Object[]> depRecords = new ArrayList<>();
    if (Files.exists(dep)) {
      try (InputStream input = new BufferedInputStream(Files.newInputStream(dep))) {
        DBFReader reader = new DBFReader(input, DBF_CHARSET);
        depFields = new DBFField[reader.getFieldCount()];
        for (int i = 0; i < depFields.length; i++) {
          depFields[i] = reader.getField(i);
        }
        Object[] row;
        while ((row = reader.nextRecord()) != null) {
          depRecords.add(row);
        }
      }
    } else {
      depFields = defaultDepFields();
    }

    try (CSVParser parser = CSVParser.parse(csv, StandardCharsets.UTF_8,
        CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
      for (CSVRecord record : parser) {
        String serie = getCsvValue(record, "Série", "Serie");
        String rgn = record.get("RGN");
        String animal = buildAnimalId(serie, rgn);

        for (String valueColumn : VALUE_COLUMNS) {
          String valueRaw = record.get(valueColumn);
          String accRaw = record.get("ACC_" + valueColumn);
          if (isBlank(valueRaw) && isBlank(accRaw)) {
            continue;
          }
          Double value = parseDouble(valueRaw);
          Double accuracy = parseDouble(accRaw);

          Integer code = descdepResult.valueCodes.get(valueColumn);
          if (code == null) {
            throw new IllegalStateException("Código não encontrado para coluna: " + valueColumn);
          }

          Object[] newRow = buildDepRow(depFields, animal, 2, code, value, accuracy);
          depRecords.add(newRow);
        }
      }
    }

    writeDbf(dep, depFields, depRecords);
  }

  private static List<CsvColumn> buildCsvColumns() {
    List<CsvColumn> columns = new ArrayList<>();
    for (String value : VALUE_COLUMNS) {
      columns.add(new CsvColumn(value, "Valor de " + value, false));
      columns.add(new CsvColumn("ACC_" + value, "Acurácia de " + value, true));
    }
    return columns;
  }

  private static DescdepResult ensureDescdep(Path path, List<CsvColumn> columns) throws IOException {
    DBFField[] fields;
    List<Object[]> records = new ArrayList<>();
    int maxCode = 0;
    Map<String, Integer> existingCodes = new HashMap<>();

    if (Files.exists(path)) {
      try (InputStream input = new BufferedInputStream(Files.newInputStream(path))) {
        DBFReader reader = new DBFReader(input, DBF_CHARSET);
        fields = new DBFField[reader.getFieldCount()];
        for (int i = 0; i < fields.length; i++) {
          fields[i] = reader.getField(i);
        }
        Object[] row;
        while ((row = reader.nextRecord()) != null) {
          records.add(row);
          Map<String, Integer> index = buildFieldIndex(fields);
          Object desc = row[index.get("DESCDEP")];
          Object code = row[index.get("CODIGODEP")];
          if (desc != null && code != null) {
            String descName = desc.toString().trim();
            int codeValue = ((Number) code).intValue();
            existingCodes.put(descName, codeValue);
            if (codeValue > maxCode) {
              maxCode = codeValue;
            }
          }
        }
      }
    } else {
      fields = defaultDescdepFields();
    }

    Map<String, Integer> valueCodes = new LinkedHashMap<>();
    Map<String, Integer> fieldIndex = buildFieldIndex(fields);

    for (CsvColumn column : columns) {
      if (existingCodes.containsKey(column.name())) {
        if (!column.isAccuracy()) {
          valueCodes.put(column.name(), existingCodes.get(column.name()));
        }
        continue;
      }

      int code = column.isAccuracy() ? 0 : ++maxCode;
      Object[] row = new Object[fields.length];
      row[fieldIndex.get("PROGAVAL")] = 2;
      row[fieldIndex.get("CODIGODEP")] = code;
      row[fieldIndex.get("DESCDEP")] = column.name();
      row[fieldIndex.get("DESCRICAO")] = column.description();
      records.add(row);
      existingCodes.put(column.name(), code);
      if (!column.isAccuracy()) {
        valueCodes.put(column.name(), code);
      }
    }

    writeDbf(path, fields, records);
    return new DescdepResult(valueCodes);
  }

  private static Map<String, Integer> buildFieldIndex(DBFField[] fields) {
    Map<String, Integer> index = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      index.put(fields[i].getName(), i);
    }
    return index;
  }

  private static Object[] buildDepRow(DBFField[] fields, String animal, int progAval, int codigoDep,
                                      Double valor, Double acuracia) {
    Object[] row = new Object[fields.length];
    Map<String, Integer> index = buildFieldIndex(fields);
    row[index.get("ANIMAL")] = animal;
    row[index.get("PROGAVAL")] = progAval;
    row[index.get("CODIGODEP")] = codigoDep;
    row[index.get("VALOR")] = valor;
    row[index.get("ACURACIA")] = acuracia;
    return row;
  }

  private static DBFField[] defaultDescdepFields() {
    DBFField progAval = new DBFField();
    progAval.setName("PROGAVAL");
    progAval.setType(DBFField.FIELD_TYPE_N);
    progAval.setLength(3);

    DBFField codigo = new DBFField();
    codigo.setName("CODIGODEP");
    codigo.setType(DBFField.FIELD_TYPE_N);
    codigo.setLength(4);

    DBFField descdep = new DBFField();
    descdep.setName("DESCDEP");
    descdep.setType(DBFField.FIELD_TYPE_C);
    descdep.setLength(20);

    DBFField descricao = new DBFField();
    descricao.setName("DESCRICAO");
    descricao.setType(DBFField.FIELD_TYPE_C);
    descricao.setLength(60);

    return new DBFField[]{progAval, codigo, descdep, descricao};
  }

  private static DBFField[] defaultDepFields() {
    DBFField animal = new DBFField();
    animal.setName("ANIMAL");
    animal.setType(DBFField.FIELD_TYPE_C);
    animal.setLength(20);

    DBFField progAval = new DBFField();
    progAval.setName("PROGAVAL");
    progAval.setType(DBFField.FIELD_TYPE_N);
    progAval.setLength(3);

    DBFField codigo = new DBFField();
    codigo.setName("CODIGODEP");
    codigo.setType(DBFField.FIELD_TYPE_N);
    codigo.setLength(4);

    DBFField valor = new DBFField();
    valor.setName("VALOR");
    valor.setType(DBFField.FIELD_TYPE_N);
    valor.setLength(12);
    valor.setDecimalCount(4);

    DBFField acuracia = new DBFField();
    acuracia.setName("ACURACIA");
    acuracia.setType(DBFField.FIELD_TYPE_N);
    acuracia.setLength(12);
    acuracia.setDecimalCount(4);

    return new DBFField[]{animal, progAval, codigo, valor, acuracia};
  }

  private static void writeDbf(Path path, DBFField[] fields, List<Object[]> records) throws IOException {
    Path temp = path.resolveSibling(path.getFileName() + ".tmp");
    try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(temp))) {
      DBFWriter writer = new DBFWriter(output, DBF_CHARSET);
      writer.setFields(fields);
      for (Object[] row : records) {
        writer.addRecord(row);
      }
      writer.close();
    }
    Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING);
  }

  private static Path createBackup(Path path) throws IOException {
    String timestamp = LocalDateTime.now().format(BACKUP_FORMATTER);
    Path backup = path.resolveSibling(path.getFileName() + ".bak-" + timestamp);
    Files.copy(path, backup, StandardCopyOption.REPLACE_EXISTING);
    return backup;
  }

  private static String buildAnimalId(String serie, String rgn) {
    String serieValue = serie == null ? "" : serie.trim();
    String rgnValue = rgn == null ? "" : rgn.trim();
    if (rgnValue.isEmpty()) {
      return serieValue;
    }

    String letters = rgnValue.replaceAll("[^A-Za-z]", "");
    String numbers = rgnValue.replaceAll("[^0-9]", "");
    if (!numbers.isEmpty()) {
      DecimalFormat formatter = new DecimalFormat("0000");
      numbers = formatter.format(Integer.parseInt(numbers));
      return serieValue + letters + numbers;
    }
    return serieValue + rgnValue;
  }

  private static Double parseDouble(String raw) {
    if (isBlank(raw)) {
      return null;
    }
    String normalized = raw.trim().replace(',', '.');
    return Double.parseDouble(normalized);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  private static String getCsvValue(CSVRecord record, String primary, String fallback) {
    if (record.isMapped(primary)) {
      return record.get(primary);
    }
    if (record.isMapped(fallback)) {
      return record.get(fallback);
    }
    return "";
  }

  private static Map<String, String> parseOptions(String[] args) {
    Map<String, String> options = new HashMap<>();
    for (int i = 1; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith("--") && i + 1 < args.length) {
        options.put(arg, args[i + 1]);
        i++;
      }
    }
    return options;
  }

  private static String getRequiredOption(Map<String, String> options, String key) {
    String value = options.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Parâmetro obrigatório ausente: " + key);
    }
    return value;
  }

  private static int parseIntOption(Map<String, String> options, String key, int defaultValue) {
    String value = options.get(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  private record CsvColumn(String name, String description, boolean isAccuracy) {
  }

  private record DescdepResult(Map<String, Integer> valueCodes) {
  }
}
