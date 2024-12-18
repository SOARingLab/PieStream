package org.piestream.piepair.dfa;

/**
 * Enum representing the set of alphabet characters used in the DFA (Deterministic Finite Automaton).
 * The alphabet consists of four characters: O, I, Z, and E.
 */
public enum Alphabet {
    O, I, Z, E;

    /**
     * Converts a character to its corresponding Alphabet enum value.
     * The comparison is case-insensitive.
     *
     * @param value the character to convert.
     * @return the corresponding Alphabet enum, or null if the character is not valid.
     */
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
                return null; // Returns null if the character is invalid
            // Alternatively, you can throw an exception here if needed
        }
    }

    /**
     * Converts a string to its corresponding Alphabet enum value.
     * The comparison is case-insensitive.
     *
     * @param value the string to convert.
     * @return the corresponding Alphabet enum, or null if the string is not valid.
     */
    public static Alphabet fromString(String value) {
        if (value == null) {
            return null; // Returns null if the string is null
            // Alternatively, you can throw an exception here if null should not be allowed
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
                return null; // Returns null if the string is not a valid alphabet character
            // Alternatively, you can throw an exception if the string is invalid
        }
    }
}
