package it.uniroma3.dia.sii.hbase.utils;


public class Tuple<A, B> {

    private final A a;

    public A getA() {
        return a;
    }

    public B getB() {
        return b;
    }

    private final B b;

    public Tuple(A a, B b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        if (!a.equals(tuple.getA())) return false;
        return b.equals(tuple.getB());
    }

    @Override
    public int hashCode() {
        int result = a.hashCode();
        result = 31 * result + b.hashCode();
        return result;
    }

    @Override
    public String toString(){
        return "< " + this.a + ", " + this.b + " >";
    }
}
