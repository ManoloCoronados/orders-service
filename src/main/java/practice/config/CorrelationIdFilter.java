package practice.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.regex.Pattern;

@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String HEADER_NAME = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    private static final Pattern VALID_CORRELATION_ID = Pattern.compile("^[A-Za-z0-9._:-]{1,100}$");
    private static final String ACTUATOR_PREFIX = "/actuator";

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return request.getRequestURI().startsWith(ACTUATOR_PREFIX);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        long startedNanos = System.nanoTime();
        String correlationId = resolveCorrelationId(request);

        try (MDC.MDCCloseable ignored = MDC.putCloseable(MDC_KEY, correlationId)) {
            response.setHeader(HEADER_NAME, correlationId);
            try {
                chain.doFilter(request, response);
            } finally {
                log.info("http request completed method={} path={} status={} durationMs={}",
                        request.getMethod(), request.getRequestURI(), response.getStatus(),
                        Duration.ofNanos(System.nanoTime() - startedNanos).toMillis());
            }
        }
    }

    private String resolveCorrelationId(HttpServletRequest request) {
        String incoming = request.getHeader(HEADER_NAME);
        if (incoming == null || incoming.isBlank()) {
            return UUID.randomUUID().toString();
        }
        String candidate = incoming.trim();
        if (VALID_CORRELATION_ID.matcher(candidate).matches()) {
            return candidate;
        }
        String generated = UUID.randomUUID().toString();
        log.warn("invalid {} header rejected, generated a new one correlationId={} receivedLength={}",
                HEADER_NAME, generated, incoming.length());
        return generated;
    }
}