package org.example.events;
import java.util.ArrayList;
import java.util.Map;

public interface PointEvent {

    // 定义获取事件值的方法，返回ArrayList
    ArrayList<String> getPayload();
}