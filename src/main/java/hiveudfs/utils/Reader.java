package hiveudfs.utils;

import com.google.common.base.Joiner;
import com.sun.tools.javac.comp.Lower;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Reader {
    protected static CsvParserSettings csvParserSettings = new CsvParserSettings();
    protected static TsvParserSettings tsvParserSettings = new TsvParserSettings();
    protected static CsvParser csvParser;
    protected static TsvParser tsvParser;

    private static Log logger = LogFactory.getLog(Lower.class);

    private static BufferedReader getReader(String uri) throws IOException {
        Path uriPath = Paths.get(uri);
        Path fullFilePath = FileSystems.getDefault().getPath(uri);
        Path fileName = fullFilePath.getFileName();
        if (Files.exists(uriPath)){
            return Files.newBufferedReader(uriPath, Charset.defaultCharset());
        }else if (Files.exists(fileName)) {
            return Files.newBufferedReader(fileName, Charset.defaultCharset());
        } else if (Files.exists(fullFilePath)) {
            return Files.newBufferedReader(fullFilePath, Charset.defaultCharset());
        } else {
            throw new IOException("Could not find \"" + fileName + "\" or \"" + fullFilePath + "\"");
        }
    }

    public static List<String[]> readCsv(String uri) throws IOException {
        csvParserSettings.getFormat().setLineSeparator("\n");
        csvParserSettings.setHeaderExtractionEnabled(false);
        csvParser = new CsvParser(csvParserSettings);

        BufferedReader br = getReader(uri);
        List<String[]> allRows = csvParser.parseAll(br);
        allRows.forEach(row -> logger.debug("read row: " + (Joiner.on(",").join(row))));
        return allRows;
    }

    public static List<String[]> readTsv(String uri) throws IOException {
        tsvParserSettings.getFormat().setLineSeparator("\n");
        tsvParserSettings.setHeaderExtractionEnabled(false);
        tsvParser = new TsvParser(tsvParserSettings);

        BufferedReader br = getReader(uri);
        List<String[]> allRows = tsvParser.parseAll(br);
        allRows.forEach(row -> logger.debug("read row: " + (Joiner.on("\t").join(row))));
        return allRows;
    }
}
