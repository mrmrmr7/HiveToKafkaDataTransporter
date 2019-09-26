package entity;

public enum Precision {
    FIVE(5), FOUR(4), THREE(3) , TWO(2), ONE(1);

    private int value;

    Precision(int value) {
        this.value = value;
    }

    public int getPrecision() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
