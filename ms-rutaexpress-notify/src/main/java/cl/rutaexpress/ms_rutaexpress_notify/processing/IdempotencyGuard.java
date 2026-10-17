package cl.rutaexpress.ms_rutaexpress_notify.processing;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IdempotencyGuard {
    private final Map<String,Long> completed = new LinkedHashMap<>();
    private final int capacity;
    private final long ttlMillis;
    private final Clock clock = Clock.systemUTC();
    public IdempotencyGuard(@Value("${notify.idempotency.capacity:10000}") int capacity,
                            @Value("${notify.idempotency.ttl-seconds:86400}") long ttlSeconds) {
        if (capacity<1 || ttlSeconds<1) throw new IllegalArgumentException("invalid_idempotency_limits");
        this.capacity=capacity; this.ttlMillis=Math.multiplyExact(ttlSeconds,1000);
    }
    @FunctionalInterface public interface Effect { void run() throws Exception; }
    // Serializes check/effect/record across the three consumers in this JVM.
    public synchronized boolean execute(String key, Effect effect) throws Exception {
        long now = clock.millis();
        completed.entrySet().removeIf(entry -> now-entry.getValue()>=ttlMillis);
        if (completed.containsKey(key)) return false;
        effect.run();
        if (completed.size()>=capacity) completed.remove(completed.keySet().iterator().next());
        completed.put(key, clock.millis());
        return true;
    }
}
