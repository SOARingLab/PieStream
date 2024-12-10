package org.piestream.events;

public class Attribute {
    private final String name;
    private final String type;

    public Attribute(String name, String type) {
        this.name = name;
        this.type = type;
    }


    public String getName() {
        return name != null ? name.trim() : null;
    }

    public String getType() {
        return type != null ? type.trim() : null;
    }

    @Override
    public String toString() {
        return "Attribute{name='" + name + "', type='" + type + "'}";
    }

    @Override
    public int hashCode() {
        return 31 * name.hashCode() + type.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Attribute attribute = (Attribute) obj;
        return name.equals(attribute.name) && type.equals(attribute.type);
    }
}
