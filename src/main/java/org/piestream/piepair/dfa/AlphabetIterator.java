package org.piestream.piepair.dfa;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class AlphabetIterator implements Iterator<Alphabet> {
    private final String sequence;
    private int currentIndex;

    public AlphabetIterator(String sequence) {
        this.sequence = sequence;
        this.currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < sequence.length();
    }

    @Override
    public Alphabet next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        char currentChar = sequence.charAt(currentIndex);
        currentIndex++;
        Alphabet alphabet = Alphabet.fromChar(currentChar);
        if (alphabet == null) {
            throw new IllegalArgumentException("Invalid character for Alphabet: " + currentChar);
        }
        return alphabet;
    }
}
