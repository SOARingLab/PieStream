package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.engine.WindowType;
import org.piestream.evaluation.Correct;
import org.piestream.events.Expirable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LinkList<T extends Expirable> {

    private static final Logger logger = LoggerFactory.getLogger(LinkList.class);
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
    private long size;     // 当前元素个数
    private final Window window;

    public LinkList( Window window) {
        this.window=window;
        this.size = 0;
        this.head = null;
        this.tail = null;
    }

    // 检查链表是否已满
    public boolean isFull() {
        return window.getWindowType() == WindowType.CAPACITY_WINDOW && size == window.getWindowCapacity();
    }

    // 检查链表是否为空
    public boolean isEmpty() {
        return size == 0;
    }


    public void safeAdd(T data) {
        // 如果链表满了，删除头部节点
        if (isFull()) {
            deleteHead();
        }
        Node newNode = new Node(data, null, null);
        this.sortedInsert(newNode);
    }

    private void sortedInsert(Node node) {
        if (isEmpty()) {
            // 如果链表为空，将新节点设为 head 和 tail
            head = node;
            tail = node;
        } else {
            Node crt = tail;

            // 向前遍历链表，找到合适的插入位置
            while (crt != null && node.getData().getSortKey() < crt.getData().getSortKey()) {
                crt = crt.prev;
            }

            if (crt == null) {
                // 插入到链表头部，成为新的 head
                node.next = head;
                head.prev = node;
                head = node;
            } else if (crt == tail) {
                // 插入到链表尾部，成为新的 tail
                crt.next = node;
                node.prev = crt;
                tail = node;
            } else {
                // 插入到中间位置
                node.next = crt.next;
                node.prev = crt;
                crt.next.prev = node;
                crt.next = node;
            }
        }
        size++;
    }



    // 删除头部节点
    public T  deleteHead() {
        if (isEmpty()) {
            logger.info("The list is empty. No elements to delete.");
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
            logger.info("The list is empty. No elements to delete.");
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
            logger.info("The list is empty.");
            return;
        }

        Node current = head;
        while (current != null) {
            System.out.print(current.data + " -> ");
            current = current.next;
        }
        logger.info("null");
    }

    // TODO: bug
    public void concat(LinkList<T > otherLinkList){
        if(otherLinkList.getSize()==0){
            return;
        }
        if(window.getWindowType() ==WindowType.CAPACITY_WINDOW && this.size+otherLinkList.getSize()>this.getCapacity()){
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
    public long getSize() {
        return size;
    }



    // 获取链表的容量
    public long getCapacity() {
        return window.getWindowCapacity();
    }

    // 获取头节点
    public Node getHead() {
        return head;
    }

    // 获取尾节点
    public Node getTail() {
        return tail;
    }

    //TIME WINDOW
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
        if (head != null) {
            head.prev = null;
        } else {
            tail = null;
        }
        size-=cnt;

        return dataList;
    }
    public List<T> deleteFromHead(long excess){
        List<T> dataList= new ArrayList<T>();
        while(excess>0){
            dataList.add(deleteHead());
            excess--;
        }
        return dataList;
    }

}
