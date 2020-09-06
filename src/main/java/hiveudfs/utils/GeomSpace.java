package hiveudfs.utils;

import java.util.Iterator;

public class GeomSpace implements Iterable<Double>, Iterator<Double> {

    private final LogSpace logspace;

    public GeomSpace(double start, double stop, int num) {
        this(start, stop, num, true);
    }

    public GeomSpace(double start, double stop, int num, boolean endpoint) {
        if (num < 0) {
            throw new IllegalArgumentException("THe argument 'num' must be non-negative.");
        }
        this.logspace = new LogSpace(Math.log10(start), Math.log10(stop), num, 10., endpoint);
    }

    @Override
    public Iterator<Double> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return logspace.hasNext();
    }

    @Override
    public Double next() {
        return logspace.next();
    }
}
