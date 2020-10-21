package hiveudfs.udf.matcher;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MatchFromFileTest {
    private MatchFromFile udf;
    private String uri1;
    private String uri2;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        udf = new MatchFromFile();

        TsvWriterSettings tsvWriterSettings = new TsvWriterSettings();
        tsvWriterSettings.getFormat().setLineSeparator("\n");
        TsvWriter tsvWriter;

        String fileName1 = "test1.tsv";
        File file1 = temporaryFolder.newFile(fileName1);
        uri1 = file1.getPath();
        tsvWriter = new TsvWriter(file1, StandardCharsets.UTF_8, tsvWriterSettings);
        tsvWriter.writeRows(new Object[][]{
                new String[]{"aa", "0"},
                new String[]{"bb", "1"},
                new String[]{"cc", "2"},
                new String[]{"dd", "3"}});
        tsvWriter.close();

        String fileName2 = "test2.tsv";
        File file2 = temporaryFolder.newFile(fileName2);
        uri2 = file2.getPath();
        tsvWriter = new TsvWriter(file2, StandardCharsets.UTF_8, tsvWriterSettings);
        tsvWriter.writeRows(new Object[][]{
                new String[]{"aa bb cc", "10"},
                new String[]{"bb cc dd", "11"},
                new String[]{"cc dd ee", "20"}});
        tsvWriter.close();
    }

    @Test
    public void testEvaluateWithPatternMatcher() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
                        TypeInfoFactory.stringTypeInfo,
                        new Text(uri1))});
        Object result;

        // No match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zaaz"), new DeferredJavaObject(uri1)});
        assertNull(resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("ccz"), new DeferredJavaObject(uri1)});
        assertNull(resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zdd"), new DeferredJavaObject(uri1)});
        assertNull(resultOI.getPrimitiveJavaObject(result));

        // Perfect match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("aa"), new DeferredJavaObject(uri1)});
        assertEquals("aa", resultOI.getPrimitiveJavaObject(result));

        // Partial match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zbbz"), new DeferredJavaObject(uri1)});
        assertEquals("bb", resultOI.getPrimitiveJavaObject(result));

        // Suffix match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("zcc"), new DeferredJavaObject(uri1)});
        assertEquals("cc", resultOI.getPrimitiveJavaObject(result));

        // Prefix match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("ddz"), new DeferredJavaObject(uri1)});
        assertEquals("dd", resultOI.getPrimitiveJavaObject(result));
    }

    @Test
    public void testEvaluateWithSetMatcher() throws Exception {
        StringObjectInspector resultOI = (StringObjectInspector) udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
                        TypeInfoFactory.stringTypeInfo,
                        new Text(uri2))});
        Object result;

        // No match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("a b c"), new DeferredJavaObject(uri2)});
        assertNull(resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("xx yy zz"), new DeferredJavaObject(uri2)});
        assertNull(resultOI.getPrimitiveJavaObject(result));

        // Subset match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("aa"), new DeferredJavaObject(uri2)});
        assertEquals("aa bb cc", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("aa bb"), new DeferredJavaObject(uri2)});
        assertEquals("aa bb cc", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("cc aa"), new DeferredJavaObject(uri2)});
        assertEquals("aa bb cc", resultOI.getPrimitiveJavaObject(result));

        // Superset match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("bb cc dd ee"), new DeferredJavaObject(uri2)});
        assertEquals("bb cc dd", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("aa dd cc bb ee"), new DeferredJavaObject(uri2)});
        assertEquals("bb cc dd", resultOI.getPrimitiveJavaObject(result));

        // No particular order match
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("cc dd ee"), new DeferredJavaObject(uri2)});
        assertEquals("cc dd ee", resultOI.getPrimitiveJavaObject(result));
        result = udf.evaluate(new DeferredObject[]{new DeferredJavaObject("dd ee cc"), new DeferredJavaObject(uri2)});
        assertEquals("cc dd ee", resultOI.getPrimitiveJavaObject(result));
    }
}
