package org.piestream.engine;


public enum WindowType {
    TIME_WINDOW,  // 时间窗口
    CAPACITY_WINDOW; // 计数窗口

    @Override
    public String toString() {
        switch(this) {
            case TIME_WINDOW: return "Time_Window";
            case CAPACITY_WINDOW: return "Count_Window";    // Not recommend:  window managed by Data Structure'capacity.
            default: throw new IllegalArgumentException();
        }
    }
}