package in._10h.java.trial.sqs.consumers.awssdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class SQSMessageConsumer implements InitializingBean, DisposableBean {

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(60);

    private static final Logger LOGGER = LoggerFactory.getLogger(SQSMessageConsumer.class);

    private final ExecutorService pool;
    private final Duration gracefulShutdownTimeout;
    private final Duration forceShutdownTimeout;
    private final SqsClient sqsClient;
    private final AtomicBoolean closed;
    private final String queueURL;

    public SQSMessageConsumer() {
        this.queueURL = "https://sqs.ap-northeast-1.amazonaws.com/129008548655/trial-sqs-consumers-awssdk";
        this.pool = Executors.newSingleThreadExecutor();
        this.gracefulShutdownTimeout = SHUTDOWN_TIMEOUT.dividedBy(2);
        this.forceShutdownTimeout = SHUTDOWN_TIMEOUT.minus(this.gracefulShutdownTimeout);
        this.sqsClient = SqsClient.create();
        this.closed = new AtomicBoolean(false);
    }

    public void start() {
        this.pool.submit(() -> {
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
                final var receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(this.queueURL)
                        .waitTimeSeconds(2)
                        .maxNumberOfMessages(10)
                        .build();
                try {
                    final var received = this.sqsClient.receiveMessage(receiveRequest);
                    final var messages = received.messages();
                    LOGGER.debug("successfully receive messages: {}", messages.size());
                    for (final var message : messages) {
                        this.handler(message);
                        final var deleteMessageReq = DeleteMessageRequest.builder()
                                .queueUrl(this.queueURL)
                                .receiptHandle(message.receiptHandle())
                                .build();
                        this.sqsClient.deleteMessage(deleteMessageReq);
                    }
                } catch (final Exception ex) {
                    LOGGER.error("exception occurred while handling message", ex);
                }

            }
        });
    }

    private void handler(final Message message) {
        LOGGER.info("Received message: {} (message id: {})", message.body(), message.messageId());
    }

    @Override
    public void destroy() throws Exception {
        this.closed.set(true);
        this.pool.shutdown();
        try {
            if (this.pool.awaitTermination(this.gracefulShutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                LOGGER.info("SQSMessageConsumer graceful shutdown completed successfully");
            } else {
                this.pool.shutdownNow();
                if (this.pool.awaitTermination(this.forceShutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    LOGGER.info("SQSMessageConsumer force shutdown completed successfully");
                } else {
                    LOGGER.warn("Failed to shutdown thread pool for SQSMessageConsumer");
                }
            }
        } catch (InterruptedException iex) {
            LOGGER.warn("SQSMessageConsumer.destroy() caller thread interrupted while shutting down thread pool; start force shutdown and re-interrupt", iex);
            this.pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.start();
    }
}
