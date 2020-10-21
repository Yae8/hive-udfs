package hiveudfs.udf.matcher;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class MatchFromFileTest {
    private MatchFromFile udf;
    private String uri1;
    private String uri2;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        udf = new MatchFromFile();

        String fileName1 = "test1.tsv";
        File file1 = temporaryFolder.newFile(fileName1);
        uri1 = file1.getPath();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        csvWriterSettings.getFormat().setLineSeparator("\n");

        CsvWriter csvWriter = new CsvWriter(file1, StandardCharsets.UTF_8, csvWriterSettings);
        csvWriter.writeRows(new Object[][]{
                new String[]{"aa", "1"},
                new String[]{"bb", "2"},
                new String[]{"cc", "3"},
                new String[]{"dd", "4"}});
        csvWriter.close();

        String fileName2 = "test2.tsv";
        File file2 = temporaryFolder.newFile(fileName2);
        uri2 = file2.getPath();
        TsvWriterSettings tsvWriterSettings = new TsvWriterSettings();
        tsvWriterSettings.getFormat().setLineSeparator("\n");
        TsvWriter tsvWriter = new TsvWriter(file2, StandardCharsets.UTF_8, tsvWriterSettings);
        tsvWriter.writeRows(new Object[][]{
                new String[]{"aa", "1"},
                new String[]{"bb", "2"},
                new String[]{"cc", "3"},
                new String[]{"dd", "4"}});
        tsvWriter.close();
    }

    @Test
    public void testEvaluate() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector});
        Object result;

        // No match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zaaz"), new DeferredJavaObject(uri2)});
        assertNull(resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("ccz"), new DeferredJavaObject(uri2)});
        assertNull(resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zdd"), new DeferredJavaObject(uri2)});
        assertNull(resultOI.getPrimitiveJavaObject(result));

        // Perfect match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("aa"), new DeferredJavaObject(uri2)});
        assertEquals("aa", resultOI.getPrimitiveJavaObject(result));

        // Partial match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zbbz"), new DeferredJavaObject(uri2)});
        assertEquals("bb", resultOI.getPrimitiveJavaObject(result));

        // Suffix match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zcc"), new DeferredJavaObject(uri2)});
        assertEquals("cc", resultOI.getPrimitiveJavaObject(result));

        // Prefix match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("ddz"), new DeferredJavaObject(uri2)});
        assertEquals("dd", resultOI.getPrimitiveJavaObject(result));
    }
}
