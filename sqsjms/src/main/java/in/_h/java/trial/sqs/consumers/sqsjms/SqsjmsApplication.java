package in._h.java.trial.sqs.consumers.sqsjms;

import jakarta.jms.Connection;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;

import software.amazon.awssdk.services.sqs.SqsClient;

@SpringBootApplication
public class SqsjmsApplication {

	public static void main(String[] args) {
		SpringApplication.run(SqsjmsApplication.class, args);
	}

	@Bean
	public SqsClient sqsClient() {
		return SqsClient.create();
	}

	@Bean(initMethod = "start", destroyMethod = "close")
	public Connection sqsConnection(
			final SqsClient sqsClient
	) throws Exception {
		final var providerConfiguration = new ProviderConfiguration();
		providerConfiguration.setNumberOfMessagesToPrefetch(10);
		final var connectionFactory = new SQSConnectionFactory(providerConfiguration, sqsClient);
		return connectionFactory.createConnection();
	}

}
