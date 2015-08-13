package demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;
import reactor.core.dispatch.ThreadPoolExecutorDispatcher;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.Streams;

import static reactor.Environment.get;

/**
 * Example service which has examples of:
 *
 * 1.)  Wrapping blocking calls by returning Promises and Streams (Reactor Project examples)
 * 2.)  Spring's @Async annotated methods that will execute in a different thread pool (async/non-blocking).
 */
@Service
public class AsyncService {

    @Autowired
    ThreadPoolExecutorDispatcher threadPoolExecutorDispatcher;

    @Async
    public void springAsync() {
        System.out.println("@async: " + Thread.currentThread());
    }

    /**
     * blocking call that simply blocks for the number of millis you pass it
     * @param millis number of millis to block
     * @return current system time as a long integer
     */
    public Long blockingCall(int millis) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return System.currentTimeMillis();
    }

    /**
     * reactor way of turning a blocking call into a non-blocking call by dispatching it into a different thread pool
     * and returning a Promise that will be fulfilled later
     * @param result
     * @return
     */
    public Promise<Long> infoBarrierCheckPromise(DeferredResult<Long> result) {
        return Promises.task(get(), threadPoolExecutorDispatcher, () -> blockingCall(100)).onComplete(p -> result.setResult(p.get()));
    }

    /**
     * reactor way of turning a blocking call into a non-blocking call by dispatching into a different thread poll
     * and returning a Reactive Stream.  You can then use the stream to modify data in a function way by calling functions
     * like map(), filter(), etc.  Thinking in signals.
     * @return
     */
    public Stream<Long> infoBarrierCheckStream() {
        return Streams.defer(() -> Streams.just(blockingCall(100))).subscribeOn(threadPoolExecutorDispatcher);
    }
}
