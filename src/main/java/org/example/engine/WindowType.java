package org.example.engine;


public enum WindowType {
    TIME_WINDOW,  // 时间窗口
    COUNT_WINDOW; // 计数窗口

    @Override
    public String toString() {
        switch(this) {
            case TIME_WINDOW: return "Time_Window";
            case COUNT_WINDOW: return "Count_Window";
            default: throw new IllegalArgumentException();
        }
    }
}