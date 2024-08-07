package org.example.events;

import java.util.ArrayList;

public class CSVPointEvent implements PointEvent {
    private final ArrayList<String> payload;

    public CSVPointEvent(ArrayList<String> payload) {
        this.payload = payload;
    }

    @Override
    public ArrayList<String> getPayload() {
        return payload;
    }
}
