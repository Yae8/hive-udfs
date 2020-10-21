package hiveudfs.udf.matcher;

import hiveudfs.utils.PatternMatcher;
import hiveudfs.utils.Reader;
import hiveudfs.utils.SetMatcher;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
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
        if (!ObjectInspectorUtils.isConstantObjectInspector(arguments[1])) {
            throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function must be a constant string" +
                    ", but " + arguments[1].toString() + "was given");
        }
        String pathname = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
        try {
            data = Reader.readTsv(pathname);
        } catch (IOException e) {
            e.printStackTrace();
            throw new UDFArgumentException("The file " + pathname + " not found");
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
            throw new HiveException("Failed to read the file");
        }

        String str = arguments[0].get().toString();
        for (String[] row : data) {
            boolean isMatch;
            MatchMethodEnum matchMethodEnum;
            if (row.length > 1) {
                matchMethodEnum = MatchMethodEnum.findByNum(Integer.parseInt(row[1]));
                if (matchMethodEnum == null) {
                    throw new HiveException("The match type " + row[1] + " unsupported");
                }
            } else {
                matchMethodEnum = MatchMethodEnum.PERFECT;
            }
            switch (matchMethodEnum) {
                case PERFECT:
                    isMatch = PatternMatcher.isPerfectMatch(str, row[0]);
                    break;
                case PARTIAL:
                    isMatch = PatternMatcher.isPartialMatch(str, row[0]);
                    break;
                case SUFFIX:
                    isMatch = PatternMatcher.isSuffixMatch(str, row[0]);
                    break;
                case PREFIX:
                    isMatch = PatternMatcher.isPrefixMatch(str, row[0]);
                    break;
                case SUBSET:
                    isMatch = SetMatcher.isSubset(str.split(" "), row[0].split(" "));
                    break;
                case SUPERSET:
                    isMatch = SetMatcher.isSuperset(str.split(" "), row[0].split(" "));
                    break;
                case NO_PARTICULAR_ORDER:
                    isMatch = SetMatcher.isEqual(str.split(" "), row[0].split(" "));
                    break;
                default:
                    throw new HiveException("The match type " + row[1] + " unsupported");
            }
            if (isMatch) {
                return new Text(row[0]);
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
