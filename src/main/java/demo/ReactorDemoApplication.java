package demo;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.context.request.async.DeferredResult;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.core.dispatch.ThreadPoolExecutorDispatcher;
import reactor.fn.tuple.Tuple2;
import reactor.spring.context.config.EnableReactor;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static reactor.Environment.initializeIfEmpty;
import static reactor.bus.selector.Selectors.$;

/**
 * Demo creating thread pool for @Async annotation as well as creating an EventBus using the Reactor project.
 *
 * There is a service and a controller that will pass requests off to the AsyncService
 */
@SpringBootApplication
@EnableAsync
@EnableReactor
public class ReactorDemoApplication implements AsyncConfigurer {

    public static final String ASYNC_TOPIC = "ASYNC_TOPIC";

    /**
     * initialize the Reactor Environment (only needs to be done once at startup
     */
    static {
        initializeIfEmpty()
                .assignErrorJournal();
    }

    /**
     * utility function to return 2^n
     * @param n
     * @return
     */
    private static int pow(int n) {
        return (int) Math.pow(2, n);
    }

    /**
     * simple example of a blocking call.  it just sleeps for the number of millis you pass it
     * @param millis
     */
    private static void blockingServiceCall(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Cached thread pool returned as an executor service because cached thread pools are fast as hell for non-blocking
     * IO - reuses threads instead of creating new ones every time.
     * @return
     */
    @Bean
    public ExecutorService cachedThreadPool() {
        return Executors.newCachedThreadPool();
    }

    /**
     * Thread pool dispatcher for running blocking code (database calls, network calls, etc.)
     * @return
     */
    @Bean
    public ThreadPoolExecutorDispatcher threadPoolExecutorDispatcher() {
        return new ThreadPoolExecutorDispatcher(pow(10), pow(10), cachedThreadPool());
    }

    /**
     * creates Reactor project EventBus
     * @return
     */
    @Bean
    public EventBus eventBus() {
        EventBus eventBus = EventBus.create(threadPoolExecutorDispatcher());

        // setup event channels
        eventBus.on($(ASYNC_TOPIC), (Event<Tuple2<DeferredResult<String>, String>> event) -> {
            blockingServiceCall(50); // blocking call to DB for instance
            System.out.println(Thread.currentThread());
            event.getData().getT1().setResult(event.getData().getT2()); // set the deffered result so the user is responded to
        });

        return eventBus;
    }

    /**
     * Sets the Executor for Spring @Async annotated methods
     * @return
     */
    @Override
    public Executor getAsyncExecutor() {
        return cachedThreadPool();
    }

    /**
     * Creates the exception handler for any exceptions thrown but not caught during an @Async task execution
     * @return
     */
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new AsyncUncaughtExceptionHandler() {
            @Override
            public void handleUncaughtException(Throwable ex, Method method, Object... params) {
                System.err.println("wtf mate? " + ex);
            }
        };
    }

    public static void main(String... args) throws InterruptedException {
        SpringApplication.run(ReactorDemoApplication.class);
    }




}

