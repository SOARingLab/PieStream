package org.piestream.piepair.dfa;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for traversing through the characters of a string sequence and mapping
 * them to the corresponding Alphabet objects.
 * This iterator converts each character in the sequence to an Alphabet using the
 * `Alphabet.fromChar` method.
 */
public class AlphabetIterator implements Iterator<Alphabet> {

    private final String sequence; // The string sequence to iterate over
    private int currentIndex; // Current index of the iterator in the sequence

    /**
     * Constructs an AlphabetIterator for the given string sequence.
     *
     * @param sequence The string sequence to iterate over.
     */
    public AlphabetIterator(String sequence) {
        this.sequence = sequence;
        this.currentIndex = 0; // Initialize the index to the start of the sequence
    }

    /**
     * Checks if there are more elements to iterate over.
     *
     * @return true if there are more elements in the sequence, false otherwise.
     */
    @Override
    public boolean hasNext() {
        return currentIndex < sequence.length(); // Return true if there are more characters
    }

    /**
     * Retrieves the next Alphabet object from the sequence.
     *
     * @return The next Alphabet object corresponding to the current character.
     * @throws NoSuchElementException If no more elements are available in the sequence.
     * @throws IllegalArgumentException If the character in the sequence is not a valid Alphabet.
     */
    @Override
    public Alphabet next() {
        if (!hasNext()) {
            throw new NoSuchElementException(); // No more elements to return
        }

        char currentChar = sequence.charAt(currentIndex); // Get the current character from the sequence
        currentIndex++; // Move the index forward

        // Convert the character to the corresponding Alphabet object
        Alphabet alphabet = Alphabet.fromChar(currentChar);
        if (alphabet == null) {
            throw new IllegalArgumentException("Invalid character for Alphabet: " + currentChar); // Invalid character
        }

        return alphabet; // Return the mapped Alphabet object
    }
}
