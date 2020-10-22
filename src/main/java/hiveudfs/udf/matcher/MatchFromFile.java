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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

public class MatchFromFile extends GenericUDF {
    protected transient List<String[]> data;
    private transient ObjectInspector stringOI;
    private transient ObjectInspector fileOI;

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

        stringOI = arguments[0];
        switch (((PrimitiveObjectInspector) stringOI).getPrimitiveCategory()) {
            case STRING:
            case CHAR:
            case VARCHAR:
            case VOID:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Argument " + 1 + " of function must be " +
                        serdeConstants.STRING_TYPE_NAME + ", " +
                        serdeConstants.CHAR_TYPE_NAME + ", " +
                        serdeConstants.VARCHAR_TYPE_NAME + "or " +
                        serdeConstants.VOID_TYPE_NAME +
                        ", but " + stringOI.getTypeName() + ".");
        }

        fileOI = arguments[1];
        if (((PrimitiveObjectInspector) fileOI).getPrimitiveCategory() != PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function must be " +
                    serdeConstants.STRING_TYPE_NAME +
                    ", but " + fileOI.getTypeName() + ".");
        }
        if (!ObjectInspectorUtils.isConstantObjectInspector(fileOI)) {
            throw new UDFArgumentTypeException(1, "Argument " + 2 + " of function must be a constant string" +
                    ", but " + fileOI.toString() + "was given");
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public String[] getRequiredFiles() {
        return new String[] {ObjectInspectorUtils.getWritableConstantValue(fileOI).toString()};
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        String string = ObjectInspectorUtils.copyToStandardJavaObject(
                arguments[0].get(), stringOI).toString();

        if (data == null) {
            String uri = ObjectInspectorUtils.copyToStandardJavaObject(
                    arguments[1].get(), fileOI).toString();
            try {
                data = Reader.readTsv(uri);
            } catch (IOException e) {
                throw new HiveException(e.getMessage());
            }
        }

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
                    isMatch = PatternMatcher.isPerfectMatch(string, row[0]);
                    break;
                case PARTIAL:
                    isMatch = PatternMatcher.isPartialMatch(string, row[0]);
                    break;
                case SUFFIX:
                    isMatch = PatternMatcher.isSuffixMatch(string, row[0]);
                    break;
                case PREFIX:
                    isMatch = PatternMatcher.isPrefixMatch(string, row[0]);
                    break;
                case SUBSET:
                    isMatch = SetMatcher.isSubset(string.split(" "), row[0].split(" "));
                    break;
                case SUPERSET:
                    isMatch = SetMatcher.isSuperset(string.split(" "), row[0].split(" "));
                    break;
                case NO_PARTICULAR_ORDER:
                    isMatch = SetMatcher.isEqual(string.split(" "), row[0].split(" "));
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
    public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
        super.copyToNewInstance(newInstance);
        MatchFromFile that = (MatchFromFile) newInstance;
        if (that != this) {
            that.stringOI = this.stringOI;
            that.fileOI = this.fileOI;
            that.data = this.data;
        }
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
