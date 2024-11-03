package org.example.merger;

import org.example.engine.WindowType;
import org.example.events.Expirable;

import java.util.ArrayList;
import java.util.List;

public class LinkList<T extends Expirable> {

    //TODO: 在TimeWindow下最好严格排序。
    // 节点类，定义链表的每个节点，包含双向指针
    public class Node {
        T data;       // 节点的数据
        Node next;    // 指向下一个节点的指针
        Node prev;    // 指向前一个节点的指针

        Node(T data, Node next, Node prev) {
            this.data = data;
            this.next = next;
            this.prev = prev;
        }
        public T getData(){
            return data;
        }
    }

    private Node head;    // 头指针
    private Node tail;    // 尾指针
    private final long capacity; // 链表的总容量
    private int size;     // 当前元素个数
    private final WindowType winType;

    // 构造函数，初始化链表，容量是传入的参数
    public LinkList(long capacity) {
        this.winType=WindowType.COUNT_WINDOW;
        this.capacity = capacity;
        this.size = 0;
        this.head = null;
        this.tail = null;
    }
    public LinkList( ) {
        this.winType=WindowType.TIME_WINDOW;
        this.capacity = 0;
        this.size = 0;
        this.head = null;
        this.tail = null;
    }

    // 检查链表是否已满
    public boolean isFull() {
        return winType == WindowType.COUNT_WINDOW && size == capacity;
    }

    // 检查链表是否为空
    public boolean isEmpty() {
        return size == 0;
    }


    // 向链表中添加元素（添加到尾部），如果已满，删除头部元素
    public void safeAdd(T data) {
        // 如果链表满了，删除头部节点
        if (isFull()) {
            deleteHead();
        }

        // 添加新节点到尾部
        Node newNode = new Node(data, null, tail);
        if (isEmpty()) {
            head = newNode;
        } else {
            tail.next = newNode;
        }
        tail = newNode;
        size++;
//        setIsTrigger();
    }

    // 删除头部节点
    public T  deleteHead() {
        if (isEmpty()) {
            System.out.println("The list is empty. No elements to delete.");
            return null;
        }
        T  deletedData=head.getData();
        // 将头指针指向下一个节点
        head = head.next;
        if (head != null) {
            head.prev = null;
        } else {
            tail = null;
        }
        size--;
        return deletedData;
    }

    // 从链表中删除一个节点
    public void deleteNode(Node node) {
        if (isEmpty()) {
            System.out.println("The list is empty. No elements to delete.");
            return;
        }

        // 如果要删除的是头节点
        if (node == head) {
            deleteHead();
            return;
        }

        // 如果要删除的是尾节点
        if (node == tail) {
            tail = tail.prev;
            if (tail != null) {
                tail.next = null;
            }
            size--;
            return;
        }

        // 如果要删除的是中间节点
        if (node.prev != null) {
            node.prev.next = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        }
        size--;
    }


    // 打印链表中的所有元素
    public void printList() {
        if (isEmpty()) {
            System.out.println("The list is empty.");
            return;
        }

        Node current = head;
        while (current != null) {
            System.out.print(current.data + " -> ");
            current = current.next;
        }
        System.out.println("null");
    }

    public void concat(LinkList<T > otherLinkList){
        if(otherLinkList.getSize()==0){
            return;
        }
        if(winType==WindowType.COUNT_WINDOW && this.size+otherLinkList.getSize()>this.capacity){
            throw new IllegalArgumentException("Excess size exceeds capacity of LinkList.");
        }

        //change head
        if (this.size==0){
            this.head=otherLinkList.head;
        }else{
            this.tail.next=otherLinkList.head;
            otherLinkList.head.prev=this.tail;
        }
        //change tail
        this.tail=otherLinkList.tail;

        //change size
        this.size+=otherLinkList.getSize();

    }

    public void clear() {
        // 清除所有节点
        head = null;
        tail = null;
        size = 0;
    }

    // 获取链表的大小
    public int getSize() {
        return size;
    }

    // 获取链表的容量
    public long getCapacity() {
        return capacity;
    }

    // 获取头节点
    public Node getHead() {
        return head;
    }

    // 获取尾节点
    public Node getTail() {
        return tail;
    }

    public List<T> refresh(long deadLine ){
        List<T> dataList= new ArrayList<T>();
        Node current=head;
        long cnt=0;
        while(current!=null && current.data.isExpired(deadLine)){
            dataList.add(current.data);
            current=current.next;
            cnt++;
        }
        head=current;
        size-=cnt;

        return dataList;
    }

}
