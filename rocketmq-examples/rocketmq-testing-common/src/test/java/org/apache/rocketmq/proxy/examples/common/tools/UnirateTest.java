package org.apache.rocketmq.proxy.examples.common.tools;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class UnirateTest {

    private Unirate unirate;

    @Before
    public void setUp() {
        unirate = new Unirate(1);
    }

    @Test
    public void testNext() throws InterruptedException {
        int QPS = 900000;
        unirate.setQps(QPS);
        AtomicLong i = new AtomicLong(0);

        for (int j = 0; j < 100; j++) {
            new Thread(() -> {
                while (true) {
                    long next = unirate.next();
                    if (i.incrementAndGet() % QPS == 0) {
                        System.out.printf("%d %3d => %d %n", System.currentTimeMillis(), next, i.get());
                    }
                    if (next > Unirate.NANOS / 2) {
                        CountDownLatch c = new CountDownLatch(1);
                        try {
                            c.await(next, TimeUnit.NANOSECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }).start();
        }
        Thread.sleep(5000);
    }

    @Test
    public void testRun() throws InterruptedException {
        int QPS = 100000;
        unirate.setQps(QPS);
        AtomicLong i = new AtomicLong(0);

        for (int j = 0; j < 100; j++) {
            new Thread(() -> {
                while (true) {
                    unirate.run(next -> {
                        if (i.incrementAndGet() % QPS == 0) {
                            System.out.printf("%d %3d => %d %n", System.currentTimeMillis(), next, i.get());
                        }
                    });
                }
            }).start();
        }
        Thread.sleep(5000);
    }
}
