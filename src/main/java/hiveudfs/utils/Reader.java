package hiveudfs.utils;

import com.google.common.base.Joiner;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Reader {
    protected static CsvParserSettings csvParserSettings = new CsvParserSettings();
    protected static TsvParserSettings tsvParserSettings = new TsvParserSettings();
    protected static CsvParser csvParser;
    protected static TsvParser tsvParser;

    public static List<String[]> readCsv(String uri) throws IOException {
        csvParserSettings.getFormat().setLineSeparator("\n");
        csvParserSettings.setHeaderExtractionEnabled(false);
        csvParser = new CsvParser(csvParserSettings);

        BufferedReader br = Files.newBufferedReader(Paths.get(uri), StandardCharsets.UTF_8);
        List<String[]> allRows = csvParser.parseAll(br);
        allRows.forEach(row -> System.out.println(Joiner.on(", ").join(row)));
        return allRows;
    }

    public static List<String[]> readTsv(String uri) throws IOException {
        tsvParserSettings.getFormat().setLineSeparator("\n");
        tsvParserSettings.setHeaderExtractionEnabled(false);
        tsvParser = new TsvParser(tsvParserSettings);

        BufferedReader br = Files.newBufferedReader(Paths.get(uri), StandardCharsets.UTF_8);
        List<String[]> allRows = tsvParser.parseAll(br);
        allRows.forEach(row -> System.out.println(Joiner.on(", ").join(row)));
        return allRows;
    }
}
