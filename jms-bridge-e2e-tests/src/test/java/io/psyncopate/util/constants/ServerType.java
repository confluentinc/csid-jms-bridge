package io.psyncopate.util.constants;

public enum ServerType {
    MASTER("Master"),
    SLAVE("Slave");

    private final String value;

    ServerType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}