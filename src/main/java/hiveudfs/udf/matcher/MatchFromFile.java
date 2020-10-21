package hiveudfs.udf.matcher;

import hiveudfs.utils.Read;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class MatchFromFile extends GenericUDF {
    protected transient List<String[]> data;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function accepts two arguments.");
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!arguments[i].getCategory().equals(Category.PRIMITIVE)) {
                throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function " + getUdfName() + " only takes a primitive" +
                        ", but " + arguments[i].getTypeName() + ".");
            }
        }

        PrimitiveObjectInspector[] primOIs = Arrays.copyOf(arguments, arguments.length, PrimitiveObjectInspector[].class);
        for (int i = 0; i < primOIs.length; i++) {
            if (!primOIs[i].getPrimitiveCategory().equals(PrimitiveCategory.STRING)) {
                throw new UDFArgumentTypeException(i, "Argument " + (i + 1) + " of function must be " +
                        "<" + serdeConstants.STRING_TYPE_NAME + ">" +
                        ", but " + primOIs[i].getTypeName() + ".");
            }
        }
        if (arguments[1] instanceof ConstantObjectInspector) {
            String pathname = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
            try {
                data = Read.readTsv(pathname);
            } catch (IOException e) {
                e.printStackTrace();
                throw new UDFArgumentException("The file " + pathname + " not found");
            }
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function accepts two arguments.");
        }

        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        if (data == null) {
            String uri = arguments[1].get().toString();
            try {
                data = Read.readTsv(uri);
            } catch (IOException e) {
                e.printStackTrace();
                throw new UDFArgumentException("The file " + uri + " not found");
            }
        }

        String str = arguments[0].get().toString();
        for (String[] row : data) {
            String regex;
            String originStr = row[0];
            if (row.length > 1) {
                if ("1".equals(row[1])) {
                    regex = "^" + originStr + "$";
                } else if ("2".equals(row[1])) {
                    regex = ".*" + originStr + ".*";
                } else if ("3".equals(row[1])) {
                    regex = ".*" + originStr + "$";
                } else if ("4".equals(row[1])) {
                    regex = "^"+ originStr + ".*";
                } else {
                    throw new HiveException("The match type " + originStr + " unsupported");
                }
            } else {
                regex = originStr;
            }
            if (str.matches(regex)) {
                return new Text(originStr);
            }
        }
        return null;
    }

    @Override
    protected String getFuncName() {
        return getClass().getSimpleName().toLowerCase();
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return getStandardDisplayString(getFuncName(), children);
    }
}
