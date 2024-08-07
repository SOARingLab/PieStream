package org.example.piepair.dfa;

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
}
