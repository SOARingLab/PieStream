//package org.example.utils;
//
//import java.util.List;
//import java.util.ArrayList;
//import org.example.piepair.IEP;
//
//public class IEPUpdateStruct {
//
//    private boolean tiggerSignal; // 0 无消息，1 有 onTriggerIEP ，2  有 onCompletedIEP，3 都有
//    private boolean completedSignal;
//    private IEP onTriggerIEP;         // 单个 IEP 对象，表示触发的 IEP
//    private List<IEP> onCompletedIEP; // IEP 对象的列表，表示已完成的 IEP
//
//    // 构造函数，初始化列表
//    public IEPUpdateStruct() {
//        this.tiggerSignal=false;
//        this.completedSignal=false;
//        this.onCompletedIEP = new ArrayList<>();  // 初始化 onCompletedIEP 为空列表
//    }
//
//    // Getter for onTriggerIEP
//    public IEP getOnTriggerIEP() {
//        return onTriggerIEP;
//    }
//
//    // Getter for onTriggerIEP
//    public boolean getTiggerSignal() {
//        return tiggerSignal;
//    }
//    // Getter for onTriggerIEP
//    public boolean getCompletedSignal() {
//        return completedSignal;
//    }
//
//    public void reset() {
//        tiggerSignal=false;
//        completedSignal=false;
//    }
//
//    public void signalTrigger() {
//        tiggerSignal=true;
//    }
//
//    public void signalCompleted() {
//        completedSignal=true;
//    }
//
//    // Setter for onTriggerIEP
//    public void setOnTriggerIEP(IEP onTriggerIEP) {
//        this.onTriggerIEP = onTriggerIEP;
//    }
//
//    // Getter for onCompletedIEP
//    public List<IEP> getOnCompletedIEP() {
//        return onCompletedIEP;
//    }
//
//    // Setter for onCompletedIEP (接受一个列表)
//    public void setOnCompletedIEP(List<IEP> onCompletedIEP) {
//        this.onCompletedIEP = onCompletedIEP;
//    }
//
//    // 添加一个 IEP 到已完成的 IEP 列表
//    public void addCompletedIEP(IEP completedIEP) {
//        this.onCompletedIEP.add(completedIEP);
//    }
//}
