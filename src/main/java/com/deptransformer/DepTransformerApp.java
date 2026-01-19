package com.deptransformer;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class DepTransformerApp {
  private static final Charset DBF_CHARSET = Charset.forName("Cp1252");
  private static final int PROGAVAL_VALUE = 2;

  private static final List<String> DEP_COLUMNS = Arrays.asList(
      "IGE",
      "DEPD_IPP",
      "ACD_IPP",
      "DEPD_PE365",
      "ACD_PE365",
      "DEPD_PE450",
      "ACD_PE450",
      "DEPD_PN",
      "ACD_PN",
      "DEPM_P120",
      "ACM_P120",
      "DEPM_P210",
      "ACM_P210",
      "DEPD_P120",
      "ACD_P120",
      "DEPD_P210",
      "ACD_P210",
      "DEPD_P365",
      "ACD_P365",
      "DEPD_P450",
      "ACD_P450",
      "DEPD_PAC",
      "ACD_PAC"
  );

  private static final List<String> VALUE_COLUMNS = Arrays.asList(
      "IGe",
      "MGTe",
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

  private static final Map<String, String> COLUMN_DESCRIPTIONS = createColumnDescriptions();

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      printUsage();
      return;
    }

    String command = args[0].toLowerCase(Locale.ROOT);
    Map<String, String> flags = parseFlags(Arrays.copyOfRange(args, 1, args.length));

    switch (command) {
      case "list":
        handleList(flags);
        break;
      case "clear":
        handleClear(flags);
        break;
      case "insert":
        handleInsert(flags);
        break;
      default:
        System.err.println("Comando desconhecido: " + command);
        printUsage();
        System.exit(1);
    }
  }

  private static void handleList(Map<String, String> flags) throws IOException {
    Path dbfPath = requirePath(flags, "--dbf");
    int limit = parseInt(flags.getOrDefault("--limit", "50"));
    int offset = parseInt(flags.getOrDefault("--offset", "0"));
    String search = flags.getOrDefault("--search", null);

    try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(dbfPath))) {
      DBFReader reader = new DBFReader(inputStream);
      reader.setCharactersetName(DBF_CHARSET.name());
      int fieldCount = reader.getFieldCount();
      List<String> fieldNames = new ArrayList<>();
      for (int i = 0; i < fieldCount; i++) {
        fieldNames.add(reader.getField(i).getName());
      }
      System.out.println("Colunas: " + String.join(", ", fieldNames));

      int index = 0;
      Object[] row;
      while ((row = reader.nextRecord()) != null) {
        if (index >= offset && index < offset + limit) {
          String line = Arrays.stream(row)
              .map(value -> Objects.toString(value, ""))
              .collect(Collectors.joining(", "));

          if (search != null && !line.contains(search)) {
            index++;
            continue;
          }

          System.out.println(String.format("%d: %s", index, line));
        }
        index++;
        if (index >= offset + limit) {
          break;
        }
      }
    }
  }

  private static void handleClear(Map<String, String> flags) throws IOException {
    Path dbfPath = requirePath(flags, "--dbf");
    if (!Files.exists(dbfPath)) {
      throw new IllegalArgumentException("Arquivo DBF não encontrado: " + dbfPath);
    }

    Path backupPath = buildBackupPath(dbfPath);
    Files.copy(dbfPath, backupPath, StandardCopyOption.REPLACE_EXISTING);
    System.out.println("Backup criado em: " + backupPath);

    DBFField[] fields = readFields(dbfPath);
    try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(dbfPath))) {
      DBFWriter writer = new DBFWriter(outputStream, DBF_CHARSET);
      writer.setFields(fields);
    }

    System.out.println("Arquivo limpo com sucesso.");
  }

  private static void handleInsert(Map<String, String> flags) throws IOException {
    Path csvPath = requirePath(flags, "--csv");
    Path depPath = requirePath(flags, "--dep");
    Path descdepPath = requirePath(flags, "--descdep");
    boolean append = Boolean.parseBoolean(flags.getOrDefault("--append", "false"));

    ensureDescdepEntries(descdepPath);

    Map<String, Integer> codigos = loadCodigoDep(descdepPath);
    DBFField[] depFields = readFields(depPath);

    List<Object[]> existingRecords = new ArrayList<>();
    if (append) {
      existingRecords = readAllRecords(depPath);
    }

    int inserted = 0;
    try (CSVParser parser = CSVParser.parse(
        Files.newBufferedReader(csvPath),
        CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreSurroundingSpaces())) {
      for (CSVRecord record : parser) {
        String animal = buildAnimalId(record.get("Série"), record.get("RGN"));
        for (String valueColumn : VALUE_COLUMNS) {

          String dbfColumn = "DEP" + valueColumn.toUpperCase();
          dbfColumn = dbfColumn.substring(0, 4) + "_" + dbfColumn.substring(4);

          String accuracyColumn = "ACC_" + valueColumn;
          String dbfAccuracyColumn = dbfColumn.replace("DEP", "AC");

          if ("MGTe".equals(valueColumn)) {
            dbfColumn = "MGT";
            dbfAccuracyColumn = "ACM_GT";
          }
          if ("IGe".equals(valueColumn)) {
            dbfColumn = "IGE";
            dbfAccuracyColumn = "ACI_GE";
          }

          String rawValue;
          try { rawValue = record.get(valueColumn); } catch (Exception ex) { continue; }
          if (rawValue == null || rawValue.trim().isEmpty()) {
            continue;
          }
          Double value = parseDouble(rawValue);
          Double accuracy = parseDouble(record.get(accuracyColumn));

          Integer codigoDep = codigos.get(dbfColumn);
          if (codigoDep == null) {
            throw new IllegalStateException("CODIGODEP ausente para coluna: " + valueColumn);
          }

          Object[] row = new Object[depFields.length];
          for (int i = 0; i < depFields.length; i++) {
            String fieldName = depFields[i].getName();
            row[i] = mapDepField(fieldName, animal, codigoDep, value, accuracy);
          }
          existingRecords.add(row);
          inserted++;
        }
      }
    }

    try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(depPath))) {
      DBFWriter writer = new DBFWriter(outputStream, DBF_CHARSET);
      writer.setFields(depFields);
      for (Object[] row : existingRecords) {
        writer.addRecord(row);
      }
      writer.close();
    }

    System.out.println("Registros inseridos: " + inserted);
  }

  private static void ensureDescdepEntries(Path descdepPath) throws IOException {
    DBFField[] fields = readFields(descdepPath);
    List<Object[]> existingRecords = readAllRecords(descdepPath);

    Map<String, Object[]> byDescdep = new HashMap<>();
    for (Object[] record : existingRecords) {
      String descdep = getFieldValue(fields, record, "DESCDEP");
      if (descdep != null) {
        byDescdep.put(descdep.trim(), record);
      }
    }

    int maxCodigo = existingRecords.stream()
        .map(record -> parseIntOptional(getFieldValue(fields, record, "CODIGODEP")))
        .filter(Optional::isPresent)
        .mapToInt(Optional::get)
        .max()
        .orElse(0);

    List<Object[]> newRecords = new ArrayList<>();
    int nextCodigo = maxCodigo + 1;

    for (String column : DEP_COLUMNS) {
      //String dbfColumn = "DEP" + column;
      //dbfColumn = dbfColumn.substring(0, 4) + "_" + dbfColumn.substring(4);

      if (byDescdep.containsKey(column)) {
        continue;
      }
      Object[] newRecord = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        String fieldName = fields[i].getName();
        Object value;
        switch (fieldName.toUpperCase(Locale.ROOT)) {
          case "PROGAVAL":
            value = PROGAVAL_VALUE;
            break;
          case "CODIGODEP":
            if (column.startsWith("ACC_")) {
              value = 0;
            } else {
              value = nextCodigo++;
            }
            break;
          case "DESCDEP":
            value = column;
            break;
          case "DESCRICAO":
            value = COLUMN_DESCRIPTIONS.getOrDefault(column, "Descrição para " + column);
            break;
          default:
            value = null;
        }
        newRecord[i] = value;
      }
      newRecords.add(newRecord);
    }

    if (newRecords.isEmpty()) {
      return;
    }

    existingRecords.addAll(newRecords);
    //Path newFilePath = Path.of("new-" + descdepPath.getFileName().toString());

    try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(descdepPath))) {
      DBFWriter writer = new DBFWriter(outputStream, DBF_CHARSET);
      writer.setFields(fields);
      for (Object[] record : existingRecords) {
        writer.addRecord(record);
      }
      writer.close();
    }
  }

  private static Map<String, Integer> loadCodigoDep(Path descdepPath) throws IOException {
    DBFField[] fields = readFields(descdepPath);
    List<Object[]> records = readAllRecords(descdepPath);
    Map<String, Integer> mapping = new HashMap<>();
    for (Object[] record : records) {
      String descdep = getFieldValue(fields, record, "DESCDEP");
      if (descdep == null) {
        continue;
      }
      Integer codigo = parseIntOptional(getFieldValue(fields, record, "CODIGODEP")).orElse(null);
      mapping.put(descdep.trim(), codigo);
    }
    return mapping;
  }

  private static Object mapDepField(String fieldName, String animal, Integer codigoDep,
                                    Double valor, Double acuracia) {
    switch (fieldName.toUpperCase(Locale.ROOT)) {
      case "ANIMAL":
        return animal;
      case "PROGAVAL":
        return PROGAVAL_VALUE;
      case "CODIGODEP":
        return codigoDep;
      case "VALOR":
        return valor;
      case "ACURACIA":
        return acuracia;
      default:
        return null;
    }
  }

  private static String buildAnimalId(String serie, String rgn) {
    String serieValue = serie == null ? "" : serie.trim();
    String rgnValue = rgn == null ? "" : rgn.trim();
    String formattedRgn = formatRgn(rgnValue);
    return serieValue + formattedRgn;
  }

  private static String formatRgn(String rgn) {
    if (rgn.isEmpty()) {
      return rgn;
    }
    String prefix = rgn.replaceAll("\\d", "");
    String digits = rgn.replaceAll("\\D", "");
    if (digits.isEmpty()) {
      return rgn;
    }
    String paddedDigits = digits.length() >= 4
        ? digits
        : String.format("%04d", Integer.parseInt(digits));
    return prefix + " " + paddedDigits;
  }

  private static DBFField[] readFields(Path dbfPath) throws IOException {
    try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(dbfPath))) {
      DBFReader reader = new DBFReader(inputStream);
      reader.setCharactersetName(DBF_CHARSET.name());
      DBFField[] fields = new DBFField[reader.getFieldCount()];
      for (int i = 0; i < reader.getFieldCount(); i++) {
        fields[i] = reader.getField(i);
      }
      return fields;
    }
  }

  private static List<Object[]> readAllRecords(Path dbfPath) throws IOException {
    List<Object[]> records = new ArrayList<>();
    try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(dbfPath))) {
      DBFReader reader = new DBFReader(inputStream);
      reader.setCharactersetName(DBF_CHARSET.name());
      Object[] row;
      while ((row = reader.nextRecord()) != null) {
        records.add(row);
      }
    }
    return records;
  }

  private static Path requirePath(Map<String, String> flags, String key) {
    String value = flags.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Parâmetro obrigatório ausente: " + key);
    }
    return Path.of(value);
  }

  private static int parseInt(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Número inválido: " + value, ex);
    }
  }

  private static Optional<Integer> parseIntOptional(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Integer.parseInt(value.trim()));
    } catch (NumberFormatException ex) {
      return Optional.empty();
    }
  }

  private static Double parseDouble(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return Double.valueOf(trimmed.replace(',', '.'));
  }

  private static String getFieldValue(DBFField[] fields, Object[] record, String name) {
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].getName().equalsIgnoreCase(name)) {
        Object value = record[i];
        return value == null ? null : value.toString();
      }
    }
    return null;
  }

  private static Path buildBackupPath(Path dbfPath) {
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    String fileName = dbfPath.getFileName().toString();
    return dbfPath.resolveSibling(fileName + ".bak." + timestamp);
  }

  private static Map<String, String> parseFlags(String[] args) {
    Map<String, String> flags = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.startsWith("--")) {
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          flags.put(arg, args[i + 1]);
          i++;
        } else {
          flags.put(arg, "true");
        }
      }
    }
    return flags;
  }

  private static void printUsage() {
    System.out.println("Uso: java -jar dep-transformer.jar <comando> [parametros]");
    System.out.println("\nComandos:");
    System.out.println("  list   --dbf <arquivo.dbf> [--limit 50] [--offset 0]");
    System.out.println("  clear  --dbf <arquivo.dbf>");
    System.out.println("  insert --csv <arquivo.csv> --dep <DEP.DBF> --descdep <descdep.dbf>");
  }

  private static Map<String, String> createColumnDescriptions() {
    Map<String, String> descriptions = new HashMap<>();
    descriptions.put("IGe", "Índice genético");
    descriptions.put("ACC_IGe", "Acurácia do índice genético");
    descriptions.put("DIPP", "Diferença esperada na produção de leite");
    descriptions.put("ACC_DIPP", "Acurácia de DIPP");
    descriptions.put("DPE365", "Diferença esperada na produção aos 365 dias");
    descriptions.put("ACC_DPE365", "Acurácia de DPE365");
    descriptions.put("DPE450", "Diferença esperada na produção aos 450 dias");
    descriptions.put("ACC_DPE450", "Acurácia de DPE450");
    descriptions.put("DPN", "Diferença esperada na produção ao nascimento");
    descriptions.put("ACC_DPN", "Acurácia de DPN");
    descriptions.put("MP120", "Média de produção aos 120 dias");
    descriptions.put("ACC_MP120", "Acurácia de MP120");
    descriptions.put("MP210", "Média de produção aos 210 dias");
    descriptions.put("ACC_MP210", "Acurácia de MP210");
    descriptions.put("DP120", "Diferença de peso aos 120 dias");
    descriptions.put("ACC_DP120", "Acurácia de DP120");
    descriptions.put("DP210", "Diferença de peso aos 210 dias");
    descriptions.put("ACC_DP210", "Acurácia de DP210");
    descriptions.put("DP365", "Diferença de peso aos 365 dias");
    descriptions.put("ACC_DP365", "Acurácia de DP365");
    descriptions.put("DP450", "Diferença de peso aos 450 dias");
    descriptions.put("ACC_DP450", "Acurácia de DP450");
    descriptions.put("DPAC", "Diferença de peso ao acabamento");
    descriptions.put("ACC_DPAC", "Acurácia de DPAC");
    return descriptions;
  }
}
