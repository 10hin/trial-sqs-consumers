package in._h.java.trial.sqs.consumers.sqsjms;

import jakarta.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class SQSJMSSessionManager implements InitializingBean, DisposableBean {

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(60);

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSJMSSessionManager.class);

    private final ExecutorService pool;
    private final Duration gracefulShutdownTimeout;
    private final Duration forceShutdownTimeout;
    private final Connection connection;
    private final Session session;
    private final MessageConsumer consumer;
    private final AtomicBoolean closed;
    private final String queueName;

    public SQSJMSSessionManager(
            final Connection connection
    ) {
        LOGGER.trace("SQSJMSSessionManager constructor called");
        this.queueName = "trial-sqs-consumers-sqsjms";
        this.pool = Executors.newSingleThreadExecutor();
        this.gracefulShutdownTimeout = SHUTDOWN_TIMEOUT.dividedBy(2);
        this.forceShutdownTimeout = SHUTDOWN_TIMEOUT.minus(this.gracefulShutdownTimeout);
        this.connection = connection;
        try {
            this.session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new InternalError("JMSException thrown while creating JMS Session", e);
        }
        try {
            this.consumer = session.createConsumer(session.createQueue(this.queueName));
        } catch (JMSException e) {
            throw new InternalError("JMSException thrown while creating JMS MessageConsumer", e);
        }
        this.closed = new AtomicBoolean(false);
    }

    private void start() {
        LOGGER.trace("SQSJMSSessionManager.start() called");
        this.pool.submit(this::handlingLoop);
    }

    private void handlingLoop() {
        while (true) {
            if (this.closed.get()) {
                LOGGER.info("Shutdown already started; stop handling");
                break;
            }
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.info("Listener loop thread interrupted; stop handling");
                break;
            }
            LOGGER.debug("Receive messages");
            try {
                final var message = this.consumer.receive(Duration.ofSeconds(2).toMillis());
                if (message != null) {
                    this.handle(message);
                    message.acknowledge();
                }
            } catch (final Exception e) {
                LOGGER.error("exception occurred while handling message", e);
            }
        }
    }

    private void handle(final Message message) throws Exception {
        if (message instanceof TextMessage) {
            final var textMessage = (TextMessage) message;
            LOGGER.info("Received message (TextMessage): {}", textMessage.getText());
        } else if (message instanceof BytesMessage) {
            final var bytesMessage = (BytesMessage) message;
            final var bodyLength = (int) bytesMessage.getBodyLength();
            final byte[] messageBody = new byte[bodyLength];
            bytesMessage.readBytes(messageBody);
            LOGGER.info("Receive message (BytesMessage as hex): {}", HexFormat.of().formatHex(messageBody));
        } else if (message instanceof ObjectMessage) {
            final var objectMessage = (ObjectMessage) message;
            LOGGER.info("Received message (ObjectMessage): {}", objectMessage.getObject());
        } else if (message instanceof MapMessage) {
            LOGGER.warn("Received message is MapMessage; can't encode as string");
        } else if (message instanceof StreamMessage) {
            LOGGER.warn("Received message is StreamMessage; can't encode as string");
        } else {
            // never enter here
            LOGGER.error("Received message is unknown type: message.getJMSType() = {}, message.getClass() = {}", message.getJMSType(), message.getClass());
        }
    }

    @Override
    public void destroy() throws Exception {
        this.closed.set(true);
        Exception firstException = null;
        try {
            this.consumer.close();
        } catch (final Exception ex) {
            if (firstException == null) {
                firstException = ex;
            } else {
                firstException.addSuppressed(ex);
            }
        }
        try {
            this.session.close();
        } catch (final Exception ex) {
            if (firstException == null) {
                firstException = ex;
            } else {
                firstException.addSuppressed(ex);
            }
        }
        try {
            this.connection.close();
        } catch (final Exception ex) {
            if (firstException == null) {
                firstException = ex;
            } else {
                firstException.addSuppressed(ex);
            }
        }
        this.pool.shutdown();
        try {
            if (this.pool.awaitTermination(this.gracefulShutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                LOGGER.info("SQSJMSSessionManager graceful shutdown completed successfully");
            } else {
                this.pool.shutdownNow();
                if (this.pool.awaitTermination(this.forceShutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    LOGGER.info("SQSJMSSessionManager force shutdown completed successfully");
                } else {
                    LOGGER.warn("Failed to shutdown thread pool for SQSJMSSessionManager");
                }
            }
        } catch (InterruptedException iex) {
            LOGGER.warn("SQSJMSSessionManager.destroy() caller thread interrupted while shutting down thread pool; start force shutdown and re-interrupt", iex);
            if (firstException == null) {
                firstException = iex;
            } else {
                firstException.addSuppressed(iex);
            }
            this.pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public void afterPropertiesSet() {
        LOGGER.trace("SQSJMSSessionManager.afterPropertiesSet() called");
        this.start();
    }
}
