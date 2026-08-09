/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvSelectionOperationsDemo {

    private static final Path DEFAULT_DATA_DIRECTORY = Paths.get("wayang-jdbc", "demo", "data");

    private static final String DEFAULT_SCHEMA = "fs";

    private static final int SAMPLE_ROW_LIMIT = 5;

    private static final int NUMERIC_SUMMARY_LIMIT = 10;

    private static final DecimalFormat DECIMAL_FORMAT =
            new DecimalFormat("0.##", DecimalFormatSymbols.getInstance(Locale.ROOT));

    public static void main(final String[] args) throws IOException {
        final Path dataDirectory = args.length > 0 ? Paths.get(args[0]) : DEFAULT_DATA_DIRECTORY;
        final String schema = args.length > 1 ? args[1] : DEFAULT_SCHEMA;

        final List<Path> csvFiles = listCsvFiles(dataDirectory);
        if (csvFiles.isEmpty()) {
            System.out.println("No CSV files found in " + dataDirectory.toAbsolutePath().normalize());
            return;
        }

        printAvailableCsvFiles(csvFiles, schema);

        final Path selectedFile = selectCsvFile(csvFiles);
        final CsvTable table = readCsv(selectedFile);

        System.out.println();
        System.out.println("Selected file: " + selectedFile.getFileName());
        System.out.println("SQL table name: " + schema + "." + tableName(selectedFile));
        System.out.println("Total rows: " + table.rows().size());

        printColumns(table);
        printSampleRows(table);
        printNumericSummaries(table);

        if (hasColumn(table, "has_heart_disease")) {
            printHeartDiseaseAnalysis(table);
        } else {
            printGenericCategoricalSummary(table);
        }
    }

    private static List<Path> listCsvFiles(final Path dataDirectory) throws IOException {
        if (!Files.isDirectory(dataDirectory)) {
            throw new IllegalArgumentException(
                    "Data directory does not exist: " + dataDirectory.toAbsolutePath().normalize()
            );
        }

        try (Stream<Path> files = Files.list(dataDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(CsvSelectionOperationsDemo::isCsvFile)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());
        }
    }

    private static void printAvailableCsvFiles(final List<Path> csvFiles, final String schema) {
        System.out.println("Available CSV files:");
        for (int index = 0; index < csvFiles.size(); index++) {
            final Path csvFile = csvFiles.get(index);
            System.out.printf(
                    Locale.ROOT,
                    "%d. %-40s SQL table: %s.%s%n",
                    index + 1,
                    csvFile.getFileName(),
                    schema,
                    tableName(csvFile)
            );
        }
    }

    private static Path selectCsvFile(final List<Path> csvFiles) {
        System.out.println();
        System.out.print("Select CSV by number or name, for example heart_disease_risk: ");

        try (Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                final String input = scanner.nextLine().trim();
                final Optional<Path> selected = resolveSelection(input, csvFiles);
                if (selected.isPresent()) {
                    return selected.get();
                }

                System.out.print("Invalid selection. Try number or CSV/table name: ");
            }
        }

        throw new IllegalArgumentException("No CSV file was selected.");
    }

    private static Optional<Path> resolveSelection(final String input, final List<Path> csvFiles) {
        if (input.isBlank()) {
            return Optional.empty();
        }

        try {
            final int selectedIndex = Integer.parseInt(input);
            if (selectedIndex >= 1 && selectedIndex <= csvFiles.size()) {
                return Optional.of(csvFiles.get(selectedIndex - 1));
            }
        } catch (NumberFormatException ignored) {
            // Continue with name-based matching.
        }

        final String normalizedInput = normalize(input);
        return csvFiles.stream()
                .filter(path -> {
                    final String fileName = normalize(path.getFileName().toString());
                    final String tableName = normalize(tableName(path));
                    return fileName.equals(normalizedInput)
                            || tableName.equals(normalizedInput)
                            || fileName.contains(normalizedInput)
                            || tableName.contains(normalizedInput);
                })
                .findFirst();
    }

    private static CsvTable readCsv(final Path csvFile) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(csvFile, StandardCharsets.UTF_8)) {
            final String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("CSV file is empty: " + csvFile);
            }

            final List<String> columns = parseCsvLine(headerLine);
            final List<List<String>> rows = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    rows.add(normalizeRow(parseCsvLine(line), columns.size()));
                }
            }
            return new CsvTable(columns, rows);
        }
    }

    private static List<String> normalizeRow(final List<String> row, final int columnCount) {
        final List<String> normalized = new ArrayList<>(row);
        while (normalized.size() < columnCount) {
            normalized.add("");
        }
        if (normalized.size() > columnCount) {
            return normalized.subList(0, columnCount);
        }
        return normalized;
    }

    private static List<String> parseCsvLine(final String line) {
        final List<String> values = new ArrayList<>();
        final StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int index = 0; index < line.length(); index++) {
            final char currentChar = line.charAt(index);
            if (currentChar == '"') {
                if (inQuotes && index + 1 < line.length() && line.charAt(index + 1) == '"') {
                    current.append('"');
                    index++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (currentChar == ',' && !inQuotes) {
                values.add(current.toString());
                current.setLength(0);
            } else {
                current.append(currentChar);
            }
        }
        values.add(current.toString());
        return values;
    }

    private static void printColumns(final CsvTable table) {
        System.out.println();
        System.out.println("Columns:");
        for (int index = 0; index < table.columns().size(); index++) {
            System.out.println((index + 1) + ". " + table.columns().get(index));
        }
    }

    private static void printSampleRows(final CsvTable table) {
        System.out.println();
        System.out.println("First " + Math.min(SAMPLE_ROW_LIMIT, table.rows().size()) + " rows:");

        for (int rowIndex = 0; rowIndex < Math.min(SAMPLE_ROW_LIMIT, table.rows().size()); rowIndex++) {
            final List<String> row = table.rows().get(rowIndex);
            System.out.println("Row " + (rowIndex + 1) + ":");
            for (int columnIndex = 0; columnIndex < table.columns().size(); columnIndex++) {
                System.out.println("  " + table.columns().get(columnIndex) + " = " + row.get(columnIndex));
            }
        }
    }

    private static void printNumericSummaries(final CsvTable table) {
        System.out.println();
        System.out.println("Numeric column summaries:");

        int printed = 0;
        for (int columnIndex = 0; columnIndex < table.columns().size(); columnIndex++) {
            final Stats stats = numericStats(table, columnIndex);
            if (stats.count() == 0) {
                continue;
            }

            System.out.printf(
                    Locale.ROOT,
                    "  %-30s count=%d min=%s max=%s avg=%s%n",
                    table.columns().get(columnIndex),
                    stats.count(),
                    format(stats.min()),
                    format(stats.max()),
                    format(stats.average())
            );

            printed++;
            if (printed >= NUMERIC_SUMMARY_LIMIT) {
                break;
            }
        }

        if (printed == 0) {
            System.out.println("  No numeric columns detected.");
        }
    }

    private static void printHeartDiseaseAnalysis(final CsvTable table) {
        System.out.println();
        System.out.println("Heart disease analysis:");
        System.out.println("This is dataset analysis only, not medical advice.");

        printCountByColumn(table, "has_heart_disease", "Patients by heart disease flag");
        printAverageByHeartDiseaseFlag(table, "age");
        printAverageByHeartDiseaseFlag(table, "bmi");
        printAverageByHeartDiseaseFlag(table, "cholesterol_total");
        printAverageByHeartDiseaseFlag(table, "daily_steps");
        printNestedCount(table, "smoker_status", "has_heart_disease", "Heart disease flag by smoker status");
        printNestedCount(table, "sex", "has_heart_disease", "Heart disease flag by sex");
    }

    private static void printGenericCategoricalSummary(final CsvTable table) {
        final Optional<String> firstCategoricalColumn = table.columns().stream()
                .filter(column -> numericStats(table, columnIndex(table, column)).count() == 0)
                .findFirst();

        firstCategoricalColumn.ifPresent(column ->
                printCountByColumn(table, column, "Counts by " + column)
        );
    }

    private static void printCountByColumn(
            final CsvTable table,
            final String columnName,
            final String title
    ) {
        final int columnIndex = columnIndex(table, columnName);
        final Map<String, Integer> counts = new LinkedHashMap<>();
        for (final List<String> row : table.rows()) {
            final String value = row.get(columnIndex);
            counts.merge(value.isBlank() ? "<blank>" : value, 1, Integer::sum);
        }

        System.out.println();
        System.out.println(title + ":");
        counts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry ->
                        System.out.println("  " + entry.getKey() + " = " + entry.getValue())
                );
    }

    private static void printAverageByHeartDiseaseFlag(final CsvTable table, final String numericColumnName) {
        if (!hasColumn(table, numericColumnName)) {
            return;
        }

        final int groupIndex = columnIndex(table, "has_heart_disease");
        final int valueIndex = columnIndex(table, numericColumnName);
        final Map<String, Stats> statsByFlag = new LinkedHashMap<>();

        for (final List<String> row : table.rows()) {
            final Optional<Double> value = parseDouble(row.get(valueIndex));
            if (value.isPresent()) {
                statsByFlag
                        .computeIfAbsent(row.get(groupIndex), ignored -> new Stats())
                        .accept(value.get());
            }
        }

        System.out.println();
        System.out.println("Average " + numericColumnName + " by heart disease flag:");
        statsByFlag.forEach((flag, stats) ->
                System.out.println("  " + flag + " = " + format(stats.average()))
        );
    }

    private static void printNestedCount(
            final CsvTable table,
            final String groupColumnName,
            final String valueColumnName,
            final String title
    ) {
        if (!hasColumn(table, groupColumnName) || !hasColumn(table, valueColumnName)) {
            return;
        }

        final int groupIndex = columnIndex(table, groupColumnName);
        final int valueIndex = columnIndex(table, valueColumnName);
        final Map<String, Map<String, Integer>> counts = new LinkedHashMap<>();

        for (final List<String> row : table.rows()) {
            final String group = row.get(groupIndex);
            final String value = row.get(valueIndex);
            counts.computeIfAbsent(group, ignored -> new LinkedHashMap<>())
                    .merge(value, 1, Integer::sum);
        }

        System.out.println();
        System.out.println(title + ":");
        counts.forEach((group, values) ->
                System.out.println("  " + group + " -> " + values)
        );
    }

    private static Stats numericStats(final CsvTable table, final int columnIndex) {
        final Stats stats = new Stats();
        for (final List<String> row : table.rows()) {
            parseDouble(row.get(columnIndex)).ifPresent(stats::accept);
        }
        return stats;
    }

    private static int columnIndex(final CsvTable table, final String columnName) {
        for (int index = 0; index < table.columns().size(); index++) {
            if (table.columns().get(index).equalsIgnoreCase(columnName)) {
                return index;
            }
        }
        throw new IllegalArgumentException("Column does not exist: " + columnName);
    }

    private static boolean hasColumn(final CsvTable table, final String columnName) {
        return table.columns().stream().anyMatch(column -> column.equalsIgnoreCase(columnName));
    }

    private static Optional<Double> parseDouble(final String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Double.parseDouble(value));
        } catch (NumberFormatException ignored) {
            return Optional.empty();
        }
    }

    private static boolean isCsvFile(final Path path) {
        return path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".csv");
    }

    private static String tableName(final Path csvFile) {
        final String fileName = csvFile.getFileName().toString();
        return fileName.substring(0, fileName.length() - ".csv".length());
    }

    private static String normalize(final String value) {
        return value.toLowerCase(Locale.ROOT).replace(".csv", "").trim();
    }

    private static String format(final double value) {
        return DECIMAL_FORMAT.format(value);
    }

    private record CsvTable(List<String> columns, List<List<String>> rows) {
    }

    private static final class Stats {

        private int count;

        private double sum;

        private double min = Double.POSITIVE_INFINITY;

        private double max = Double.NEGATIVE_INFINITY;

        void accept(final double value) {
            this.count++;
            this.sum += value;
            this.min = Math.min(this.min, value);
            this.max = Math.max(this.max, value);
        }

        int count() {
            return this.count;
        }

        double min() {
            return this.min;
        }

        double max() {
            return this.max;
        }

        double average() {
            if (this.count == 0) {
                return 0.0;
            }
            return this.sum / this.count;
        }
    }
}
