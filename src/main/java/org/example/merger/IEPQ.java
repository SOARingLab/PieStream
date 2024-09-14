package org.example.merger;

import org.example.utils.CircularQueue;
import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

public class IEPQ {
    private   CircularQueue<IEP> circularQueue;
    private final EBA formerPred;
    private final EBA latterPred;

    // Constructor to initialize the CircularQueue and predicates
    public IEPQ(int capacity, EBA formerPred, EBA latterPred) {
        this.circularQueue = new CircularQueue<>(capacity);
        this.formerPred = formerPred;
        this.latterPred = latterPred;
    }

    public CircularQueue<IEP> getQ() {
        return circularQueue;
    }

    // Method to add an IEP to the queue
    public void enqueue(IEP iep) {
        circularQueue.enqueue(iep);
    }

    // Method to remove an IEP from the queue
    public IEP dequeue() {
        return circularQueue.dequeue();
    }

    // Method to peek at the front IEP in the queue
    public IEP peek() {
        return circularQueue.peek();
    }

    // Method to check if the queue is full
    public boolean isFull() {
        return circularQueue.isFull();
    }

    // Method to check if the queue is empty
    public boolean isEmpty() {
        return circularQueue.isEmpty();
    }

    // Method to print the queue elements
    public void printQueue() {
        circularQueue.printQueue();
    }

    // Get the capacity of the CircularQueue
    public int capacity() {
        return circularQueue.capacity();
    }

    // Get the number of elements in the CircularQueue
    public int size() {
        return circularQueue.size();
    }

    // Get the former predicate
    public EBA getFormerPred() {
        return formerPred;
    }

    // Get the latter predicate
    public EBA getLatterPred() {
        return latterPred;
    }

    // Example of version change function on queue
    public void versionChange() {
        circularQueue.versionChange();
    }

    public boolean isVersionChanged() {
        return circularQueue.isVersionChange();
    }
}
