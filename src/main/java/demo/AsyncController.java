package demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.async.DeferredResult;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.tuple.Tuple2;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Example of Reactor EventBus for async method invocation, as well as Spring's @Async annotation
 */
@Controller
public class AsyncController {

    @Autowired
    EventBus eventBus;

    @Autowired
    AsyncService asyncService;

    @RequestMapping("/async")
    @ResponseBody
    public DeferredResult<String> asyncOperation(HttpServletRequest req, HttpServletResponse resp, @RequestParam("data") String data) {
        // print the thread that is handling this request.  the next calls will execute in different threadpools
        System.out.println("Receiving request: " + Thread.currentThread());

        // send request off to another thread inside the eventBus to make a blocking call and fulfill the DeferredResult
        DeferredResult<String> result = new DeferredResult<>();
        eventBus.notify(ReactorDemoApplication.ASYNC_TOPIC, Event.wrap(Tuple2.of(result, data))); // notify the event bus

        // example call a spring async service function that will run inside a different thread pool
        asyncService.springAsync();

        return result;
    }
}
