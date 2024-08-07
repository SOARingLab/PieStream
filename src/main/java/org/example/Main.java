package org.example;

import org.example.piepair.dfa.DFA;
import org.example.piepair.dfa.Node;
import org.example.piepair.dfa.Dot2DFA;
import org.example.piepair.dfa.Alphabet;
import org.example.piepair.dfa.AlphabetIterator;

public class Main {
    public static void main(String[] args) {
        // 解析DOT文件创建DFA
        DFA dfa = Dot2DFA.parseDotFile("src/main/resources/dotFiles/OVERLAPS.dot");

        if (dfa == null) {
            System.out.println("Failed to load DFA from DOT file.");
            return;
        }
        System.out.println(dfa.getRelation());
        String sequence = "ZZEEZEZEZZEZEIIIIIIIEZEZZEZEIEZIIEZEZZEZEIEIEZE";
        long startTime = System.currentTimeMillis();

        AlphabetIterator iterator = new AlphabetIterator(sequence);

        while (iterator.hasNext()) {
            Alphabet  alphabet = iterator.next();
            Node previousNode = dfa.getCurrentState();
            Node currentNode = dfa.step(alphabet);
            String transition = alphabet.toString();
            boolean isFinal = dfa.isFinalState();
            boolean isTriggered = dfa.isTrigger();
            boolean isCompleted = dfa.isCompleted();
            System.out.println(previousNode + " -------(" + transition + ")--------> " + currentNode + " (" + (isFinal ? "Final" : "Not Final") + ")");
            if (isTriggered) {
                System.out.println("Trigger detected!");
            }
            if (isCompleted) {
                System.out.println("State completed!");
            }
        }
    }
}
