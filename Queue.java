import java.util.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

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


    public String msg_get(String topic, long time) throws InterruptedException {
        semConsumers.get(topic).acquire();
        r_mutex.acquire();
        String message = queues.get(topic).poll();
        System.out.println("after cosume: " + queues.get(topic) + " at time: " + (System.currentTimeMillis() - time));
        numMessages.get(topic).decrementAndGet();
        semProducers.get(topic).release();
        r_mutex.release();
        return message;
    }

    public boolean tryAcquire(Semaphore semaphore) throws InterruptedException {
        if (semaphore.availablePermits() > 0) {
            semaphore.acquire();
            return true;
        }
        return false;
    }

    public String nb_msg_get(String topic, long time) {
        try {
            if (tryAcquire(semConsumers.get(topic))) {
                r_mutex.acquire();
                String message = queues.get(topic).poll();
                r_mutex.release();
                if (message != null) {
                    numMessages.get(topic).decrementAndGet();
                    semProducers.get(topic).release();
                    System.out.println("after cosume nb: "+queues.get(topic)+ " at time: " + (System.currentTimeMillis() - time));
                    return message;
                } else {
                    semConsumers.get(topic).release();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private int getTotalLength(String topic) {
        int totalLength = 0;
        for (String message : queues.get(topic)) {
            totalLength += message.length();
        }
        return totalLength;
    }

    private long getMemoryUsage() {
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = mem.getHeapMemoryUsage();
        MemoryUsage nheap = mem.getNonHeapMemoryUsage();
        
        long heapUsed = heap.getUsed();
        long nonHeapUsed = nheap.getUsed();
        long total = heapUsed + nonHeapUsed;
        return total;
    }

    public Stats stats(String topic, long time) throws InterruptedException {
        Stats stats = new Stats();
        mutex.acquire();
        r_mutex.acquire();
        stats.numMessages = numMessages.get(topic).get();
        stats.totalLength = getTotalLength(topic);
        stats.memoryUsage = getMemoryUsage();
        stats.numTopics = getNumTopics();
        stats.queue = queues.get(topic);
        stats.volume = totalVolume.get(topic);
        stats.time = System.currentTimeMillis() - time;
        r_mutex.release();
        mutex.release();
        return stats;
    }

    public int getNumTopics() {
        return queues.keySet().size();
    }


}

class Stats {
    int numMessages;
    int totalLength;
    long memoryUsage;
    int numTopics;

    LinkedList<String> queue;
    int volume;
    long time;
}
