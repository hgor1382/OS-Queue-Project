import java.util.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

Class MassageQueue implements Serializable{
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

    public void saveQueues(String name) {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(name))) {
            out.writeInt(capacity);
            out.writeInt(maxVolume);
            out.writeObject(semProducers);
            out.writeObject(semConsumers);
            out.writeObject(queues);
            out.writeObject(numMessages);
            out.writeObject(totalVolume);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public MessageQueue readQueues(String name) {
        MessageQueue savedQueue = null;
    
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(name))) {
            int savedCap = (int) in.readInt();
            int savedMax = (int) in.readInt();
            Map<String, Semaphore> savedSemp = (Map<String, Semaphore>) in.readObject();
            Map<String, Semaphore> savedSemc = (Map<String, Semaphore>) in.readObject();
            Map<String, LinkedList<String>> savedQueues = (Map<String, LinkedList<String>>) in.readObject();
            Map<String, AtomicInteger> savedNum = (Map<String, AtomicInteger>) in.readObject();
            Map<String, Integer> savedTotalVolume = (Map<String, Integer>) in.readObject();
            
            savedQueue = new MessageQueue(savedCap, savedMax);
            savedQueue.semProducers.putAll(savedSemp);
            savedQueue.semConsumers.putAll(savedSemc);
            savedQueue.queues.putAll(savedQueues);
            savedQueue.numMessages.putAll(savedNum);
            savedQueue.totalVolume.putAll(savedTotalVolume);
        } catch (Exception e) {
            e.printStackTrace();
        }
    
        return savedQueue;
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

public class project {
    public static void main(String[] args) throws InterruptedException {     
        MessageQueue messageQueue = new MessageQueue(2,10000);
        messageQueue.createTopic("topic 1");
        messageQueue.createTopic("topic 2");

        long startTime = System.currentTimeMillis();

        Thread producer1 = new Thread(() -> {
            try {
                messageQueue.msg_send("topic 1","Message 1 from Producer 1", startTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread producer2 = new Thread(() -> {
            try {
                messageQueue.msg_send("topic 1","Message 2 from Producer 2", startTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        
        Thread producer3 = new Thread(() -> {
            try {
                messageQueue.msg_send("topic 1","Message 3 from Producer 3", startTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        
        Thread producer4 = new Thread(() -> {
            try {
                messageQueue.msg_send("topic 1","Message 4 from Producer 4", startTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread consumer1 = new Thread(() -> {
            try {
                String message = messageQueue.msg_get("topic 1", startTime);
                System.out.println("Consumer 1 received: " + message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        Thread consumer2 = new Thread(() -> {
            try {
                String message = messageQueue.msg_get("topic 1", startTime);
                System.out.println("Consumer 2 received: " + message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        
        Thread consumer5 = new Thread(() -> {
            try {
                String message = messageQueue.msg_get("topic 1", startTime);
                System.out.println("Consumer 5 received: " + message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        
        Thread consumer3 = new Thread(() -> {
            String message = messageQueue.nb_msg_get("topic 1", startTime);
            System.out.println("Consumer 3 received: " + message);
        });

        Thread consumer4 = new Thread(() -> {
            String message = messageQueue.nb_msg_get("topic 1", startTime);
            System.out.println("Consumer 4 received: " + message);
        });

        Thread stats1 = new Thread(() -> {
            try {
                Stats s = messageQueue.stats("topic 1", startTime);
                System.out.println("num messages: " + s.numMessages + " total length: " + s.totalLength + " mem: " + s.memoryUsage + " at time: " + s.time);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread stats2 = new Thread(() -> {
            try {
                Stats s = messageQueue.stats("topic 1", startTime);
                System.out.println("num messages: " + s.numMessages + " total length: " + s.totalLength + " mem: " + s.memoryUsage + " at time: " + s.time);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread stats3 = new Thread(() -> {
            try {
                Stats s = messageQueue.stats("topic 1", startTime);
                System.out.println("num messages: " + s.numMessages + " total length: " + s.totalLength + " mem: " + s.memoryUsage + " at time: " + s.time);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread stats4 = new Thread(() -> {
            try {
                Stats s = messageQueue.stats("topic 1", startTime);
                System.out.println("num messages: " + s.numMessages + " total length: " + s.totalLength + " mem: " + s.memoryUsage + " at time: " + s.time);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        

        //Test 1:

        // consumer1.start();
        // System.out.println(messageQueue.stats("topic 1" , startTime).queue);
        // Thread.sleep(1000);
        // producer1.start();

        //Test 2: some producers, one consumer

        // producer1.start();
        // producer2.start();
        // Thread.sleep(100);
        // consumer1.start();


        // Test 3: some consumers, one producer

        // producer3.start();
        // Thread.sleep(100);
        // consumer2.start();
        // consumer1.start();


        //Test 4:non-blocking
        
        // consumer3.start();
        // Thread.sleep(100);
        // producer1.start();
        // producer2.start();
        // Thread.sleep(100);
        // consumer4.start();
        // Thread.sleep(100);
        // consumer1.start();



        //Test 5: capacity 2

        // producer1.start();
        // producer2.start();
        // producer3.start();
        // producer4.start();
        // stats1.start();
        // Thread.sleep(100);
        // consumer1.start();
        // Thread.sleep(100);
        // consumer2.start();
        // stats2.start();


        // Test 6:
        
        // consumer1.start();
        // consumer2.start();
        // consumer5.start();
        
        // producer1.start();
        // stats1.start();
        // producer2.start();
        // producer3.start();
        // stats2.start();
        // producer4.start();
       


        //Test 7: saving capacity 3

        // producer1.start();
        // producer2.start();
        // producer3.start();

        // Thread.sleep(100);
        // messageQueue.saveQueues("save.dat");

        // MessageQueue ms = messageQueue.readQueues("save.dat");
        
        // Thread producer5 = new Thread(() -> {
        //     try {
        //         ms.msg_send("topic 1","Message 5 from Producer 5", startTime);
        //     } catch (InterruptedException e) {
        //         e.printStackTrace();
        //     }
        // });

        // Thread consumer6 = new Thread(() -> {
        //     String message = ms.nb_msg_get("topic 1", startTime);
        //     System.out.println("Consumer 6 received: " + message);
        // });

        // Thread consumer7 = new Thread(() -> {
        //     String message = ms.nb_msg_get("topic 1", startTime);
        //     System.out.println("Consumer 7 received: " + message);
        // });
        // System.out.println(ms.stats("topic 1",startTime).queue);

        // producer5.start();
        // Thread.sleep(500);
        
        // consumer6.start();
        // consumer7.start();
    }
}
