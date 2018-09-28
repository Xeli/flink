package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannel;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

class DefaultPubSubSubscriberFactory implements PubSubSubscriberFactory {
	private String hostAndPort;

	void withHostAndPort(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}

	@Override
	public Subscriber getSubscriber(CredentialsProvider credentialsProvider, ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver) {
		FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
																	.setMaxOutstandingElementCount(10000L)
																	.setMaxOutstandingRequestBytes(100000L)
																	.setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
																	.build();
		Subscriber.Builder builder = Subscriber
				.newBuilder(ProjectSubscriptionName.of(projectSubscriptionName.getProject(), projectSubscriptionName.getSubscription()), messageReceiver)
				.setFlowControlSettings(flowControlSettings)
				.setCredentialsProvider(credentialsProvider);

		if (hostAndPort != null) {
			ManagedChannel managedChannel = ManagedChannelBuilder
					.forTarget(hostAndPort)
					.usePlaintext() // This is 'Ok' because this is ONLY used for testing.
					.build();
			TransportChannel transportChannel = GrpcTransportChannel.newBuilder().setManagedChannel(managedChannel).build();
			builder.setChannelProvider(FixedTransportChannelProvider.create(transportChannel));
		}

		return builder.build();
	}
}
