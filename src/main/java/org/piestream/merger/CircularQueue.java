package org.piestream.merger;

public class CircularQueue<T> {
    private final T[] queue;
    private int front;  // 指向队列头部的索引
    private int rear;   // 指向队列尾部的下一个索引
    private int size;   // 当前队列中的元素个数
    private final int capacity;
    private int version; // 版本号
    private int latVersion; // 版本号
    @SuppressWarnings("unchecked")
    public CircularQueue(int capacity) {
        this.capacity = capacity;
        this.queue = (T[]) new Object[capacity];
        this.front = 0;
        this.rear = 0;
        this.size = 0;
        this.version=0;
        this.latVersion=0;
    }

    public void versionChange(){
        latVersion=version;
        version++;
    }
    public boolean isVersionChange(){
        return !(latVersion==version);
    }

    // 检查队列是否为空
    public boolean isEmpty() {
        return size == 0;
    }

    // 检查队列是否已满
    public boolean isFull() {
        return size == capacity;
    }

    // 向队列中添加元素
    public void enqueue(T element) {
        if (isFull()) {
            // 如果队列已满，移除头部元素
            dequeue();
        }
        queue[rear] = element;
        rear = (rear + 1) % capacity;
        size++;
    }

    // 从队列中移除并返回头部元素
    public T dequeue() {
        if (isEmpty()) {
            throw new IllegalStateException("Queue is empty");
        }
        T element = queue[front];
        queue[front] = null; // 帮助垃圾回收
        front = (front + 1) % capacity;
        size--;
        return element;
    }

    // 查看队列头部元素
    public T peek() {
        if (isEmpty()) {
            throw new IllegalStateException("Queue is empty");
        }
        return queue[front];
    }

    // 获取队列中的元素个数
    public int size() {
        return size;
    }

    // 获取队列的容量
    public int capacity() {
        return capacity;
    }

    // 从队列尾部向前搜索
    public T searchFromRear(int n) {
        if (n <= 0 || n > size) {
            throw new IllegalArgumentException("Invalid search index");
        }
        int index = (rear - n + capacity) % capacity;
        return queue[index];
    }

    // 打印队列中的所有元素
    public void printQueue() {
        if (isEmpty()) {
            System.out.println("Queue is empty");
        } else {
            for (int i = 0; i < size; i++) {
                System.out.print(queue[(front + i) % capacity] + " \n");
            }
            System.out.println();
        }
    }

}
