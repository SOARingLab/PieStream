package org.piestream.events;

public class Attribute {
    private final String name;  // The name of the attribute
    private final String type;  // The type of the attribute (e.g., int, string)

    /**
     * Constructs an Attribute with a given name and type.
     *
     * @param name The name of the attribute
     * @param type The type of the attribute
     */
    public Attribute(String name, String type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Gets the name of the attribute, ensuring that any extra spaces are removed.
     *
     * @return The name of the attribute
     */
    public String getName() {
        return name != null ? name.trim() : null;
    }

    /**
     * Gets the type of the attribute, ensuring that any extra spaces are removed.
     *
     * @return The type of the attribute
     */
    public String getType() {
        return type != null ? type.trim() : null;
    }

    /**
     * Returns a string representation of the Attribute.
     *
     * @return A string representing the Attribute object
     */
    @Override
    public String toString() {
        return "Attribute{name='" + name + "', type='" + type + "'}";
    }

    /**
     * Computes the hash code for the Attribute object based on its name and type.
     *
     * @return The hash code of the Attribute
     */
    @Override
    public int hashCode() {
        return 31 * name.hashCode() + type.hashCode();
    }

    /**
     * Checks if two Attribute objects are equal by comparing their name and type.
     *
     * @param obj The object to compare
     * @return True if the objects are equal, otherwise false
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Attribute attribute = (Attribute) obj;
        return name.equals(attribute.name) && type.equals(attribute.type);
    }
}
