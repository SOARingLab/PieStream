package org.piestream.piepair.dfa;

public enum Alphabet {
    O, I, Z, E;

    public static Alphabet fromChar(char value) {
        switch (Character.toUpperCase(value)) {
            case 'O':
                return O;
            case 'I':
                return I;
            case 'Z':
                return Z;
            case 'E':
                return E;
            default:
                return null; // Or you can throw an exception
        }
    }
    // Static method to create an Alphabet from a String
    public static Alphabet fromString(String value) {
        if (value == null) {
            return null; // Or throw an exception if null should not be allowed
        }

        switch (value.toUpperCase()) {
            case "O":
                return O;
            case "I":
                return I;
            case "Z":
                return Z;
            case "E":
                return E;
            default:
                return null; // Or throw an exception if the string is not valid
        }
    }

}
