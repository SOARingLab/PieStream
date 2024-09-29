package org.example.utils;

import org.example.merger.CircularQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CircularQueueTest {

    private CircularQueue<Integer> queue;

    @BeforeEach
    void setUp() {
        queue = new CircularQueue<>(5); // 创建一个容量为5的循环队列
    }

    @Test
    void testEnqueueDequeue() {
        // 测试队列是否为空
        assertTrue(queue.isEmpty(), "Queue should be empty initially");

        // 向队列中添加元素
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);
        queue.enqueue(4);
        queue.enqueue(5);

        // 队列应已满
        assertTrue(queue.isFull(), "Queue should be full after adding 5 elements");

        // 依次出队并检查元素
        assertEquals(1, queue.dequeue(), "First dequeued element should be 1");
        assertEquals(2, queue.dequeue(), "Second dequeued element should be 2");
        assertEquals(3, queue.dequeue(), "Third dequeued element should be 3");

        // 队列应未满
        assertFalse(queue.isFull(), "Queue should not be full after dequeuing 3 elements");

        // 向队列中再次添加元素
        queue.enqueue(6);
        queue.enqueue(7);

        // 继续出队并检查元素
        assertEquals(4, queue.dequeue(), "Fourth dequeued element should be 4");
        assertEquals(5, queue.dequeue(), "Fifth dequeued element should be 5");
        assertEquals(6, queue.dequeue(), "Sixth dequeued element should be 6");
        assertEquals(7, queue.dequeue(), "Seventh dequeued element should be 7");

        // 队列应为空
        assertTrue(queue.isEmpty(), "Queue should be empty after dequeuing all elements");
    }

    @Test
    void testSearchFromRear() {
        // 向队列中添加元素
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);
        queue.enqueue(4);
        queue.enqueue(5);

        // 搜索队列中的元素
        assertEquals(5, queue.searchFromRear(1), "Last element should be 5");
        assertEquals(4, queue.searchFromRear(2), "Second last element should be 4");
        assertEquals(3, queue.searchFromRear(3), "Third last element should be 3");
        assertEquals(2, queue.searchFromRear(4), "Fourth last element should be 2");
        assertEquals(1, queue.searchFromRear(5), "Fifth last element should be 1");

        // 测试边界条件
        assertThrows(IllegalArgumentException.class, () -> queue.searchFromRear(0), "Index 0 should throw exception");
        assertThrows(IllegalArgumentException.class, () -> queue.searchFromRear(6), "Index greater than size should throw exception");
    }

    @Test
    void testEnqueueOverflow() {
        // 向队列中添加元素
        queue.enqueue(1);
        queue.enqueue(2);
        queue.enqueue(3);
        queue.enqueue(4);
        queue.enqueue(5);

        // 队列此时已满，我们继续添加一个新元素6
        queue.enqueue(6);

        // 检查队列中的元素是否正确
        assertEquals(2, queue.dequeue(), "First element should be 2 after overflow");
        assertEquals(3, queue.dequeue(), "Second element should be 3 after overflow");
        assertEquals(4, queue.dequeue(), "Third element should be 4 after overflow");
        assertEquals(5, queue.dequeue(), "Fourth element should be 5 after overflow");
        assertEquals(6, queue.dequeue(), "Fifth element should be 6 after overflow");

        // 队列应为空
        assertTrue(queue.isEmpty(), "Queue should be empty after dequeuing all elements");
    }
    @Test
    void testDequeueUnderflow() {
        // 队列为空时应抛出异常
        assertThrows(IllegalStateException.class, () -> queue.dequeue(), "Dequeue from an empty queue should throw exception");
    }
}
