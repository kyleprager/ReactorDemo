package demo.example;

import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.core.Dispatcher;
import reactor.core.DispatcherSupplier;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.dispatch.ThreadPoolExecutorDispatcher;
import reactor.core.dispatch.WorkQueueDispatcher;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.YieldingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.Control;
import reactor.rx.broadcast.Broadcaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static reactor.Environment.*;
import static reactor.bus.selector.Selectors.$;

/**
 * Created by kyle.prager on 8/4/15.
 */
public class ReactorExamples {

    static {
        initializeIfEmpty()
                .assignErrorJournal();
    }

    public static void main(String... args) throws InterruptedException {
        // call any of the example functions here.
        eventBusExample();
    }

    private static int pow(int n) {
        return (int) Math.pow(2, n);
    }

    /////////////////////////////////////////////////////////////////////////////////////
    ////////  EXAMPLE FUNCTIONS FOR THE REACTOR AND DIFFERENT DISPATCHING METHODS  //////
    /////////////////////////////////////////////////////////////////////////////////////




    private static void eventBusExample() throws InterruptedException {

        Dispatcher dispatcher = new WorkQueueDispatcher("work_pool", pow(10), pow(10), t -> System.out.println(t), ProducerType.MULTI, new YieldingWaitStrategy());
        int n = 1000000;
        EventBus bus = EventBus.create(dispatcher);
        CountDownLatch latch = new CountDownLatch(n);
        bus.on($("topic"), (Event<Integer> ev) -> {
            latch.countDown();
            if (ev.getData() % 100000 == 0 || ev.getData() == 0) {
                System.out.println(ev.getData() + " " + Thread.currentThread() + " " + Thread.activeCount());
            }
        });

        long a = System.currentTimeMillis();
        for(int i = 0; i < n; i++) {
            bus.notify("topic", Event.wrap(i));
        }
        latch.await();
        System.out.println("ops/sec: " + (n * 1.0) / ((System.currentTimeMillis() - a) / 1000.0) );
        System.out.println("millis:  " + (System.currentTimeMillis() - a));
    }

    private static void cachedThreadPool() throws InterruptedException {
        int n = 10000000;
        CountDownLatch latch = new CountDownLatch(n);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());

        long a = System.currentTimeMillis();
        for(int i = 0; i < n; i++) {
            executor.submit(() -> {
                blockingServiceCall(100);
                latch.countDown();
            });
        }
        latch.await();
        System.out.println("ops/sec: " + (n * 1.0) / ((System.currentTimeMillis() - a) / 1000.0));
        System.exit(0);
    }

    private static void dispatcherWithCachedThreadPool() throws InterruptedException {
        Dispatcher dispatcher = new ThreadPoolExecutorDispatcher(pow(16), 0, Executors.newCachedThreadPool());
        int n = 1000000;
        CountDownLatch latch = new CountDownLatch(n);
        Consumer consumer = new Consumer<Long>() {
            @Override
            public void accept(Long aLong) {
                blockingServiceCall(100);
                latch.countDown();
            }
        };
        Consumer tc = new Consumer<Throwable>() {
            @Override
            public void accept(Throwable o) {

            }
        };
        long a = System.currentTimeMillis();
        for(int i = 0; i < n; i++) {

            dispatcher.dispatch(1l, consumer, tc);
        }
        latch.await();
        System.out.println("ops/sec: " + (n * 1.0) / ((System.currentTimeMillis() - a) / 1000.0) );
    }

    private static void ringBufferWorkProcessor(int n) throws InterruptedException {
        RingBufferWorkProcessor<Long> processor = RingBufferWorkProcessor.create(Executors.newCachedThreadPool(), pow(16));
        Stream<Long> stream = Streams.wrap(processor);

        CountDownLatch latch = new CountDownLatch(n);
        for(int j = 0; j < 1500; j++) {
            stream.consume(num -> {
                blockingServiceCall(100);
                latch.countDown();
            });
        }
        long a = System.currentTimeMillis();
        for(int j = 0; j < n; j++) {
            processor.onNext(new Long(j));
        }
        latch.await();
        System.out.println("ops/sec: " + (n * 1.0) / ((System.currentTimeMillis() - a) / 1000.0));
    }

    private static void threadPoolExecutorDispatcher(int numCalls, int blockingTimeMillis, Dispatcher dispatcher) throws InterruptedException {
        Random r = new Random();
        RingBufferWorkProcessor<Long> processor = RingBufferWorkProcessor.create();

        CountDownLatch latch = new CountDownLatch(numCalls);
        Streams.wrap(processor).consume(s -> {
            System.out.println(Thread.currentThread() + " - " + s + " - " + latch.getCount());
        });
        long start = System.currentTimeMillis();
        for(int i = 0; i < numCalls; i++) {
            dispatcher.<Long>dispatch(i * 1l, consumer -> {
                        blockingServiceCall(blockingTimeMillis);
//                        processor.onNext(consumer);
                        latch.countDown();
                    },
                    consumer -> consumer.printStackTrace(System.out));
        }
        latch.await(3, TimeUnit.SECONDS);
        System.out.println(String.format("%s calls blocking for %s millis finished in: %s", numCalls, blockingTimeMillis, (System.currentTimeMillis() - start)));
    }

    private static void flatMap() throws InterruptedException {
        long start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Streams
                .range(1, 100)
                .flatMap(number -> Streams.range(1, number).subscribeOn(workDispatcher()))
                .log()
                .concatMap(number -> Streams.range(1, number).subscribeOn(workDispatcher()))
                .take(100)
//                .reduce(0l, (a, b) -> {
//                    return a + b;
//                })
                .consume(
                        System.out::println,
                        Throwable::printStackTrace,
                        avoid -> {
                            System.out.println("--complete--");
                            countDownLatch.countDown();
                        }
                ).requestMore(1l);
        countDownLatch.await();
        System.out.println("That took " + (System.currentTimeMillis() - start) + " ms");
    }

    public static void window() throws InterruptedException {
        //create a list of 1000 numbers and prepare a Stream to read it
        Stream<Long> sensorDataStream = Streams.range(1, 1000);

        //wait for all windows of 100 to finish
        CountDownLatch endLatch = new CountDownLatch(1000 / 100);

        Control controls = sensorDataStream
                .window(100)
                .consume(window -> {
                    System.out.println("New window starting");
                    window
                            .map(n -> new Long(n).intValue())
                            .reduce(Integer.MAX_VALUE, (acc, next) -> Math.min(acc, next))
                            .finallyDo(o -> endLatch.countDown())
                            .consume(i -> System.out.println("Minimum " + i));
                });

        endLatch.await(10, TimeUnit.SECONDS);
        System.out.println(controls.debug());

    }

    /**
     * mocked blocking service call that simply sleeps for the number of millis you pass to it
     * @param millis number of millis to sleep for
     * @return system time in millis
     */
    public static long blockingServiceCall(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return System.currentTimeMillis();
    }

    private static void parallizeBlockingCalls(int numCalls, final int blockingTimeMillis) throws InterruptedException {
        Random r = new Random();
        CountDownLatch latch = new CountDownLatch(numCalls);
        DispatcherSupplier dispatcherSupplier = newCachedDispatchers(numCalls > 128 ? 128 : numCalls, "blocking_calls_pool");
        for (int i = 0; i < numCalls; i++) {
            dispatcherSupplier.get(); // pre-allocate our dispatchers
        }

        // create our non-blocking, async, reactive, ReactiveStreams stream
        Stream<Long> longStream = Streams
                .range(1, numCalls)
                .partition(numCalls)
                .flatMap(stream -> stream
                        .dispatchOn(dispatcherSupplier.get())
                        .map(n -> blockingServiceCall(blockingTimeMillis)))
                .observe(consumer -> latch.countDown());
        long start = System.currentTimeMillis();
        longStream.consume();
        latch.await(1, TimeUnit.SECONDS);
        System.out.println(String.format("%s calls blocking for %s millis finished in: %s", numCalls, blockingTimeMillis, (System.currentTimeMillis() - start)));
    }

    public static void notSoMuchWin(int numCalls, int blockingTimeMillis, ExecutorService executor) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(numCalls);
        long start = System.currentTimeMillis();
        for(int i = 0; i < numCalls; i ++) {
            executor.submit(() -> {
                blockingServiceCall(blockingTimeMillis);
                latch.countDown();
            });
        }
        latch.await(3, TimeUnit.SECONDS);
        System.out.println(String.format("%s calls blocking for %s millis finished in: %s", numCalls, blockingTimeMillis, (System.currentTimeMillis() - start)));
    }

    /**
     * makes HTTP get calls in parallel in a non-blocking, async fashion
     * @param n number of urls to get async
     * @throws InterruptedException
     */
    private static void asyncUrlGet(int n) throws InterruptedException {

        List<String> urls = new ArrayList<>(n);
        for(int i = 0; i < n; i++) {
            urls.add("http://www.google.com?param=" + i);
        }

        CountDownLatch latch = new CountDownLatch(n);
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse response) throws IOException {
                return !response.getStatusCode().is2xxSuccessful();
            }

            @Override
            public void handleError(ClientHttpResponse response) throws IOException {
                System.out.println("error on the wire: " + response.getStatusText());
            }
        });

        long start = System.currentTimeMillis();
        Streams
                .from(urls)
                .groupBy(s -> s)
                .flatMap(stream ->
                                stream
                                        .dispatchOn(cachedDispatcher())
                                        .map(s -> restTemplate.getForObject(s, String.class))
                )
                .dispatchOn(sharedDispatcher())
                .consume(
                        s -> { // consumer
                            latch.countDown();
                            System.out.println("---complete in stream---");
                        },
                        t -> { // error consumer
                            latch.countDown();
                            System.out.println("---exception---");
                            System.out.println(t);
                        },
                        s -> { // complete consumer
                            latch.countDown();
                            System.out.println("---complete---");
                        });
        latch.await();
        long finalms = (System.currentTimeMillis() - start);
        System.out.println("millis: " + finalms);
        System.out.println("millis/url: " + finalms / urls.size());
        System.out.println("---latch satisfied---");
    }

    private static void unboundedStream() {
        int limit = 1000000;

        Broadcaster<Integer> sink = Broadcaster.create(get(),
                new RingBufferDispatcher("yolo", (int) Math.pow(2, 10)));

        long start = System.currentTimeMillis();
        sink
                .consume(s -> {
                    if (s == limit) {
                        System.out.println(s);
                        System.out.println("time: " + (System.currentTimeMillis() - start));
                    }
                });
        for (int i = 0; i <= limit; i++) {
            sink.onNext(i);
        }
    }
}
