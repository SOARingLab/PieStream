package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.engine.WindowType;
import org.piestream.events.Expirable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LinkList<T extends Expirable> {

    private static final Logger logger = LoggerFactory.getLogger(LinkList.class);

    // Node class represents each element in the doubly linked list
    public class Node {
        T data;       // The data contained in the node
        Node next;    // Pointer to the next node in the list
        Node prev;    // Pointer to the previous node in the list

        // Constructor to initialize a new node
        Node(T data, Node next, Node prev) {
            this.data = data;
            this.next = next;
            this.prev = prev;
        }

        // Getter for the data in the node
        public T getData(){
            return data;
        }
    }

    private Node head;    // Pointer to the first node in the list (head)
    private Node tail;    // Pointer to the last node in the list (tail)
    private long size;     // The current number of elements in the list
    private final Window window;  // Window object to manage the capacity and type

    // Constructor to initialize the LinkList with a specified window
    public LinkList(Window window) {
        this.window = window;
        this.size = 0;
        this.head = null;
        this.tail = null;
    }

    // Checks whether the list is full based on the window's capacity
    public boolean isFull() {
        return window.getWindowType() == WindowType.CAPACITY_WINDOW && size == window.getWindowCapacity();
    }

    // Checks whether the list is empty
    public boolean isEmpty() {
        return size == 0;
    }

    // Safely adds a new element to the list, removing the head if necessary
    public void safeAdd(T data) {
        // If the list is full, remove the head element
        if (isFull()) {
            deleteHead();
        }
        // Create a new node with the provided data and insert it in sorted order
        Node newNode = new Node(data, null, null);
        this.sortedInsert(newNode);
    }

    // Inserts a node into the list in a sorted manner based on the data's sort key
    private void sortedInsert(Node node) {
        if (isEmpty()) {
            // If the list is empty, set the new node as both the head and the tail
            head = node;
            tail = node;
        } else {
            Node crt = tail;

            // Traverse the list from the tail to find the correct insertion point
            while (crt != null && node.getData().getSortKey() < crt.getData().getSortKey()) {
                crt = crt.prev;
            }

            if (crt == null) {
                // Insert the node at the head of the list
                node.next = head;
                head.prev = node;
                head = node;
            } else if (crt == tail) {
                // Insert the node at the tail of the list
                crt.next = node;
                node.prev = crt;
                tail = node;
            } else {
                // Insert the node in the middle of the list
                node.next = crt.next;
                node.prev = crt;
                crt.next.prev = node;
                crt.next = node;
            }
        }
        size++;
    }

    // Deletes the head node of the list and returns its data
    public T deleteHead() {
        if (isEmpty()) {
            logger.info("The list is empty. No elements to delete.");
            return null;
        }
        T deletedData = head.getData();
        // Update the head pointer to the next node
        head = head.next;
        if (head != null) {
            head.prev = null;
        } else {
            tail = null;
        }
        size--;
        return deletedData;
    }

    // Deletes a specific node from the list
    public void deleteNode(Node node) {
        if (isEmpty()) {
            logger.info("The list is empty. No elements to delete.");
            return;
        }

        // If the node is the head, delete the head
        if (node == head) {
            deleteHead();
            return;
        }

        // If the node is the tail, delete the tail
        if (node == tail) {
            tail = tail.prev;
            if (tail != null) {
                tail.next = null;
            }
            size--;
            return;
        }

        // For a middle node, update the previous and next pointers
        if (node.prev != null) {
            node.prev.next = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        }
        size--;
    }

    // Prints all elements in the list
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

    // Concatenates another LinkList to the current list
    public void concat(LinkList<T> otherLinkList) {
        if (otherLinkList.getSize() == 0) {
            return;
        }
        // Check if the combined size exceeds the capacity of the list
        if (window.getWindowType() == WindowType.CAPACITY_WINDOW && this.size + otherLinkList.getSize() > this.getCapacity()) {
            throw new IllegalArgumentException("Excess size exceeds capacity of LinkList.");
        }
        // Change head if the current list is empty
        if (this.size == 0) {
            this.head = otherLinkList.head;
        } else {
            this.tail.next = otherLinkList.head;
            otherLinkList.head.prev = this.tail;
        }
        // Update the tail of the list
        this.tail = otherLinkList.tail;

        // Update the size of the list
        this.size += otherLinkList.getSize();
    }

    // Clears the entire list by setting head, tail to null and size to 0
    public void clear() {
        head = null;
        tail = null;
        size = 0;
    }

    // Returns the current size of the list
    public long getSize() {
        return size;
    }

    // Returns the capacity of the list based on the window's capacity
    public long getCapacity() {
        return window.getWindowCapacity();
    }

    // Returns the head node of the list
    public Node getHead() {
        return head;
    }

    // Returns the tail node of the list
    public Node getTail() {
        return tail;
    }

    // TIME WINDOW: Removes expired data based on the provided deadline and returns the expired items
    public List<T> refresh(long deadLine) {
        List<T> dataList = new ArrayList<T>();
        Node current = head;
        long cnt = 0;

        // Traverse the list and collect all expired data
        while (current != null && current.data.isExpired(deadLine)) {
            dataList.add(current.data);
            current = current.next;
            cnt++;
        }

        // Update the head pointer to the first non-expired node
        head = current;
        if (head != null) {
            head.prev = null;
        } else {
            tail = null;
        }
        size -= cnt;

        return dataList;
    }

    // Deletes elements from the head until the excess limit is reached, and returns the deleted elements
    public List<T> deleteFromHead(long excess) {
        List<T> dataList = new ArrayList<T>();
        while (excess > 0) {
            dataList.add(deleteHead());
            excess--;
        }
        return dataList;
    }

}
