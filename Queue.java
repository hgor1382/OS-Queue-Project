import java.util.*;
import java.io.*;

Class MassageQueue{
    private final Map<String, LinkedList<String>> queues;
    private final int capacity;
    private final Map<String, Semaphore> semProducers;
    private final Map<String, Semaphore> semConsumers;
    private final Map<String, AtomicInteger> numMessages;
    private final Map<String, Integer> totalVolume;
    private final Semaphore mutex;
    private final Semaphore r_mutex;
    private final int maxVolume;

    public MessageQueue(int capacity, int maxVolume) {
        this.capacity = capacity;
        this.maxVolume = maxVolume;
        this.queues = new HashMap<>();
        this.semProducers = new HashMap<>();
        this.semConsumers = new HashMap<>();
        this.numMessages = new HashMap<>();
        this.totalVolume = new HashMap<>();
        this.mutex = new Semaphore(1);
        this.r_mutex = new Semaphore(1);
    }

    public MessageQueue(int maxVolume) {
        this.capacity = Integer.MAX_VALUE;
        this.maxVolume = maxVolume;
        this.queues = new HashMap<>();
        this.semProducers = new HashMap<>();
        this.semConsumers = new HashMap<>();
        this.numMessages = new HashMap<>();
        this.totalVolume = new HashMap<>();
        this.mutex = new Semaphore(1);
        this.r_mutex = new Semaphore(1);
    }

    public MessageQueue() {
        this.capacity = Integer.MAX_VALUE;
        this.maxVolume = Integer.MAX_VALUE;
        this.queues = new HashMap<>();
        this.semProducers = new HashMap<>();
        this.semConsumers = new HashMap<>();
        this.numMessages = new HashMap<>();
        this.totalVolume = new HashMap<>();
        this.mutex = new Semaphore(1);
        this.r_mutex = new Semaphore(1);
    }

    public void createTopic(String topic) {
        if (!queues.containsKey(topic)) {
            queues.put(topic, new LinkedList<>());
            semProducers.put(topic, new Semaphore(capacity));
            semConsumers.put(topic, new Semaphore(0));
            numMessages.put(topic, new AtomicInteger(0));
            totalVolume.put(topic, 0);
        }
    }

    public void msg_send(String topic, String message, long time) throws InterruptedException {
        semProducers.get(topic).acquire();
        mutex.acquire();
        if (totalVolume.get(topic) + message.length() > maxVolume) {
            semProducers.get(topic).release();
            return;
        }
        queues.get(topic).add(message);
        System.out.println("after produce: "+queues.get(topic) + " at time: "+ (System.currentTimeMillis() - time));
        numMessages.get(topic).incrementAndGet();
        totalVolume.put(topic, totalVolume.get(topic) + message.length());
        semConsumers.get(topic).release();
        mutex.release();
    }


}
