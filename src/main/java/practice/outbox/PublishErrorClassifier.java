package practice.outbox;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

final class PublishErrorClassifier {

    enum Kind { RETRYABLE, FATAL }

    private PublishErrorClassifier() {
    }

    static Kind classify(Throwable error) {
        Throwable root = unwrap(error);

        // Reintentar no cambia el resultado: datos invalidos o permisos.
        if (root instanceof RecordTooLargeException
                || root instanceof SerializationException
                || root instanceof AuthorizationException
                || root instanceof AuthenticationException
                || root instanceof InvalidTopicException
                || root instanceof IllegalArgumentException) {
            return Kind.FATAL;
        }
        // Transitorios: timeouts, lider no disponible, red. Tambien lo desconocido:
        // el tope de intentos acota el riesgo.
        return Kind.RETRYABLE;
    }

    static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof ExecutionException || current instanceof CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    static boolean isKnownRetriable(Throwable error) {
        return unwrap(error) instanceof RetriableException;
    }
}