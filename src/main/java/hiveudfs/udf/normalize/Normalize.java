package hiveudfs.udf.normalize;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.Normalizer;

public final class Normalize extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) { return null; }
        return new Text(Normalizer.normalize(s.toString().replaceAll(" {2,}", " "), Normalizer.Form.NFKC));
    }

    private String trim(String s) {
        return s.trim().replaceAll("\\s+", " ");
    }

    public String normalize(String s, Form form) {
        if (form == Form.NFD) {
            return Normalizer.normalize(trim(s), Normalizer.Form.NFD);
        } else if (form == Form.NFC) {
            return Normalizer.normalize(trim(s), Normalizer.Form.NFC);
        } else if (form == Form.NFKD) {
            return Normalizer.normalize(trim(s), Normalizer.Form.NFKD);
        } else if (form == Form.NFKC) {
            return Normalizer.normalize(trim(s), Normalizer.Form.NFKC);
        } else {
            return null;
        }
    }

    public String normalize(String s) {
        return normalize(trim(s), Form.NFKC);
    }

    public Text evaluate(final Text s, String formName) throws IllegalAccessException {
        if (s == null) { return null; }
        Form form = Form.getByName(formName);
        return new Text(normalize(s.toString(), form));
    }

    public Text evaluate(final Text s, int formId) throws IllegalAccessException {
        if (s == null) { return null; }
        Form form = Form.getById(formId);
        return new Text(normalize(s.toString(), form));
    }
}
