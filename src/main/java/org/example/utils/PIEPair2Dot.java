package org.example.utils;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;

public class PIEPair2Dot {

    private String followedByRegex = ".*(Z)+(O)+(I)+";
    private String meetsRegex = ".*(Z)+(I)+";
    private String overlapsRegex = ".*(Z)+(E)+(I)+";
    private String startsRegex = ".*(O)+(E)+(I)+";
    private String duringRegex = ".*(I)+(E)+(I)+";
    private String finishesRegex = ".*(I)+(E)+(O)+";
    private String equalsRegex = ".*(O)+(E)+(O)+";
    private String followRegex = ".*(I)+(O)+(Z)+";
    private String metByRegex = ".*(I)+(Z)+";
    private String overlapedByRegex = ".*(I)+(E)+(Z)+";
    private String startedByRegex = ".*(O)+(E)+(Z)+";
    private String containsRegex = ".*(Z)+(E)+(Z)+";
    private String finishedByRegex = ".*(Z)+(E)+(O)+";

    // 将 DFA 序列化为二进制文件
    public void saveDfaToBinaryFile(String filename, Automaton automaton) {
        try (FileOutputStream fileOut = new FileOutputStream(filename);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(automaton);
            System.out.println("DFA has been serialized to " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 将 DFA 输出为 DOT 文件
    public void saveDfaToDotFile(String filename, Automaton automaton) {
        try (PrintWriter writer = new PrintWriter(filename)) {
            writer.write(automaton.toDot());
            System.out.println("DFA has been written to DOT file " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void processRegexes() {
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.getType().equals(String.class)) {
                try {
                    String regex = (String) field.get(this);
                    String baseFilename = field.getName().replaceAll("Regex$", "");
                    RegExp regExp = new RegExp(regex);
                    Automaton automaton = regExp.toAutomaton();
                    automaton.determinize();

                    saveDfaToBinaryFile(baseFilename + ".dfa", automaton);
                    saveDfaToDotFile(baseFilename + ".dot", automaton);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        PIEPair2Dot piePair2Dot = new PIEPair2Dot();
        piePair2Dot.processRegexes();
    }
}
