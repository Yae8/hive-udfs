package hiveudfs.udf.normalize;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Comparator;


public class OrderedNormalize extends UDF {
    private Normalize normalize = new Normalize();

    private String sort(String s, boolean ascending) {
        String[] split = s.split(" ");
        if (ascending) {
            Arrays.sort(split, Comparator.naturalOrder());
        } else {
            Arrays.sort(split, Comparator.reverseOrder());
        }
        return String.join(" ", split);
    }

    public Text evaluate(final Text s) throws IllegalAccessException {
        if (s == null) { return null; }
        String normalized = normalize.normalize(s.toString());

        if (normalized == null) { return null; }
        return new Text(sort(normalized, true));
    }

    public Text evaluate(final Text s, boolean ascending) throws IllegalAccessException {
        if (s == null) { return null; }
        String normalized = normalize.normalize(s.toString());

        if (normalized == null) { return null; }
        return new Text(sort(normalized, ascending));
    }

    public Text evaluate(final Text s, String formName) throws IllegalAccessException {
        if (s == null) { return null; }
        Form form = Form.getByName(formName);
        String normalized = normalize.normalize(s.toString(), form);

        if (normalized == null) { return null; }
        return new Text(sort(normalized, true));
    }

    public Text evaluate(final Text s, String formName, boolean ascending) throws IllegalAccessException {
        if (s == null) { return null; }
        Form form = Form.getByName(formName);
        String normalized = normalize.normalize(s.toString(), form);

        if (normalized == null) { return null; }
        return new Text(sort(normalized, ascending));
    }

    public Text evaluate(final Text s, int formId) throws IllegalAccessException {
        if (s == null) { return null; }
        Form form = Form.getById(formId);
        String normalized = normalize.normalize(s.toString(), form);

        if (normalized == null) { return null; }
        return new Text(sort(normalized, true));
    }

    public Text evaluate(final Text s, int formId, boolean ascending) throws IllegalAccessException {
        if (s == null) { return null; }
        Form form = Form.getById(formId);
        String normalized = normalize.normalize(s.toString());

        if (normalized == null) { return null; }
        return new Text(sort(normalized, ascending));
    }
}
