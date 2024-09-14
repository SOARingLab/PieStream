//package org.example.utils;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.example.piepair.IEP;
//
//public class IEPRow {
//
//    private List<IEP> iepList;
//    private final int colNum;  // 固定的列数
//
//    // 构造函数，指定列数
//    public IEPRow(int colNum) {
//        this.colNum = colNum;
//        this.iepList = new ArrayList<>(colNum);
//    }
//
//    // 添加 IEP 到行
//    public boolean addIEP(IEP iep) {
//        if (iepList.size() >= colNum) {
//            System.out.println("Row is full. Cannot add more IEPs.");
//            return false;
//        }
//        iepList.add(iep);
//        return true;
//    }
//
//    // 添加一个较小行（小于等于当前列数的行）的 IEP 列表到当前行
//    public boolean addIEPList(IEPRow smallerIEPRow) {
//        if (smallerIEPRow.size() + this.size() > colNum) {
//            System.out.println("Cannot add IEP list. Combined size exceeds the row capacity.");
//            return false;
//        }
//
//        this.iepList.addAll(smallerIEPRow.getIEPList());
//        return true;
//    }
//
//    // 获取行中的 IEP 列表
//    public List<IEP> getIEPList() {
//        return iepList;
//    }
//
//    // 检查行是否已满
//    public boolean isFull() {
//        return iepList.size() == colNum;
//    }
//
//    // 返回行中的 IEP 数量
//    public int size() {
//        return iepList.size();
//    }
//
//    // 打印行中的所有 IEP
//    public void print() {
//        for (IEP iep : iepList) {
//            System.out.print(iep + "\t");
//        }
//        System.out.println();
//    }
//
//    @Override
//    public String toString() {
//        StringBuilder sb = new StringBuilder();
//        for (IEP iep : iepList) {
//            sb.append(iep.toString()).append("\t");  // 将每个 IEP 转为字符串并用制表符分隔
//        }
//        return sb.toString().trim();  // 移除末尾多余的制表符
//    }
//}