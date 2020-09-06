package hiveudfs.udf.normalize;

public enum Form {
    NFD(1, "NFD"),
    NFC(2, "NFC"),
    NFKD(3, "NFKD"),
    NFKC(4, "NFKC"),

    ;

    private int id;
    private String name;

    Form(int id, String name) {
        this.id = id;
        this.name = name;
    }

    private int getId() {
        return id;
    }

    private String getName() {
        return name;
    }

    public static Form getById(int id) throws IllegalAccessException {
        for (Form form : values()) {
            if (form.getId() == id) {
                return form;
            }
        }
        throw new IllegalAccessException("no such the id: " + id);
    }


    public static Form getByName(String name) throws IllegalAccessException {
        for (Form form : values()) {
            if (form.getName().equals(name)) {
                return form;
            }
        }
        throw new IllegalAccessException("no such the name: " + name);
    }

}
