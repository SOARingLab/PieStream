package org.example.utils;

public class LinkList<T> {

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
    }

    private Node head;    // 头指针
    private Node tail;    // 尾指针
    private int capacity; // 链表的总容量
    private int size;     // 当前元素个数
//    private boolean isTrigger;     // 当前元素个数

    // 构造函数，初始化链表，容量是传入的参数
    public LinkList(int capacity) {
        this.capacity = capacity;
        this.size = 0;
        this.head = null;
        this.tail = null;
//        this.isTrigger = false;
    }


//    public void resetIsTrigger() {
//        this.isTrigger =false;
//    }
//
//    public boolean getIsTrigger() {
//        return isTrigger;
//    }
//
//    public void setIsTrigger() {
//        this.isTrigger =true;
//    }

    // 检查链表是否已满
    public boolean isFull() {
        return size == capacity;
    }

    // 检查链表是否为空
    public boolean isEmpty() {
        return size == 0;
    }

    // 向链表中添加元素（添加到尾部），如果已满，删除头部元素
    public void add(T data) {
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
    public void deleteHead() {
        if (isEmpty()) {
            System.out.println("The list is empty. No elements to delete.");
            return;
        }

        // 将头指针指向下一个节点
        head = head.next;
        if (head != null) {
            head.prev = null;
        } else {
            tail = null;
        }
        size--;
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

    // 按值删除节点
    public void delete(T data) {
        if (isEmpty()) {
            System.out.println("The list is empty. No elements to delete.");
            return;
        }

        Node current = head;

        // 找到要删除的元素
        while (current != null && !current.data.equals(data)) {
            current = current.next;
        }

        if (current == null) {
            System.out.println("Element not found in the list.");
            return;
        }

        // 使用 deleteNode 方法删除找到的节点
        deleteNode(current);
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


    // 在 LinkList 类中实现 concat 方法
    public void concat(LinkList<T> other) {
        // 遍历 other 表中的每个元素，并将它们添加到当前表中
        Node current = other.head;

        // 依次将 other 的每个节点添加到当前链表中
        while (current != null) {
            // 如果当前链表的大小已达到容量，删除头部元素以腾出空间
            if (this.size >= this.capacity) {
                this.deleteHead();
            }
            // 将当前节点的数据添加到链表中
            this.add(current.data);

            // 移动到 other 链表中的下一个节点
            current = current.next;
        }
    }

    public void clear() {
        // 清除所有节点
        head = null;
        tail = null;
        size = 0;
//        System.out.println("The list has been cleared.");
    }


//
//    // 从尾部向前搜索第 n 个元素
//    public T searchFromRear(int n) {
//        if (n <= 0 || n > size) {
//            throw new IllegalArgumentException("Invalid value of n: " + n);
//        }
//
//        Node current = tail;
//        for (int i = 1; i < n; i++) {
//            current = current.prev;
//        }
//        return current.data;
//    }

    // 获取链表的大小
    public int getSize() {
        return size;
    }

    // 获取链表的容量
    public int getCapacity() {
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
}
