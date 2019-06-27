/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeIdsForCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.AcknowledgeOnCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.Acknowledger;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;

/**
 * PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge them on the next checkpoint.
 * This ensures every message will get acknowledged at least once.
 */
public class PubSubSource<OUT> extends RichSourceFunction<OUT>
	implements Acknowledger<String>, ResultTypeQueryable<OUT>, ParallelSourceFunction<OUT>, CheckpointListener, ListCheckpointed<AcknowledgeIdsForCheckpoint<String>> {
	public static final int NO_MAX_MESSAGES_TO_ACKNOWLEDGE_LIMIT = -1;
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);
	protected final PubSubDeserializationSchema<OUT> deserializationSchema;
	protected final PubSubSubscriberFactory pubSubSubscriberFactory;
	protected final Credentials credentials;
	protected final int maxMessagesToAcknowledge;
	protected final AcknowledgeOnCheckpointFactory acknowledgeOnCheckpointFactory;

	protected transient AcknowledgeOnCheckpoint<String> acknowledgeOnCheckpoint;
	protected transient PubSubSubscriber subscriber;

	protected transient volatile boolean isRunning;

	PubSubSource(PubSubDeserializationSchema<OUT> deserializationSchema,
				 PubSubSubscriberFactory pubSubSubscriberFactory,
				 Credentials credentials,
				 int maxMessagesToAcknowledge,
				 AcknowledgeOnCheckpointFactory acknowledgeOnCheckpointFactory) {
		this.deserializationSchema = deserializationSchema;
		this.pubSubSubscriberFactory = pubSubSubscriberFactory;
		this.credentials = credentials;
		this.maxMessagesToAcknowledge = maxMessagesToAcknowledge;
		this.acknowledgeOnCheckpointFactory = acknowledgeOnCheckpointFactory;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		if (hasNoCheckpointingEnabled(getRuntimeContext())) {
			throw new IllegalArgumentException("The PubSubSource REQUIRES Checkpointing to be enabled and " +
				"the checkpointing frequency must be MUCH lower than the PubSub timeout for it to retry a message.");
		}

		getRuntimeContext().getMetricGroup().gauge("PubSubMessagesProcessedNotAcked", this::getOutstandingMessagesToAck);

		createAndSetAcknowledgeOnCheckpoint();
		this.subscriber = pubSubSubscriberFactory.getSubscriber(credentials);
		this.isRunning = true;
	}

	private boolean hasNoCheckpointingEnabled(RuntimeContext runtimeContext) {
		return !(runtimeContext instanceof StreamingRuntimeContext && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled());
	}

	@Override
	public void acknowledge(List<String> acknowledgementIds) {
		if (!isRunning) {
			return;
		}

		subscriber.acknowledge(acknowledgementIds);
	}

	@Override
	public void run(SourceContext<OUT> sourceContext) throws Exception {
		while (isRunning) {
			try {
				blockIfMaxMessagesToAcknowledgeLimitReached();

				processMessage(sourceContext, subscriber.pull());
			} catch (InterruptedException | CancellationException e) {
				isRunning = false;
			}
		}
		subscriber.close();
	}

	void processMessage(SourceContext<OUT> sourceContext, List<ReceivedMessage> messages) throws Exception {
		synchronized (sourceContext.getCheckpointLock()) {
			for (ReceivedMessage message : messages) {
				acknowledgeOnCheckpoint.addAcknowledgeId(message.getAckId());

				PubsubMessage pubsubMessage = message.getMessage();

				OUT deserializedMessage = deserializationSchema.deserialize(pubsubMessage);
				if (deserializationSchema.isEndOfStream(deserializedMessage)) {
					cancel();
					return;
				}

				sourceContext.collect(deserializedMessage);
			}

		}
	}

	private void blockIfMaxMessagesToAcknowledgeLimitReached() throws Exception {
		while (maxMessagesToAcknowledge != NO_MAX_MESSAGES_TO_ACKNOWLEDGE_LIMIT && getOutstandingMessagesToAck() > maxMessagesToAcknowledge) {
			LOG.debug("Sleeping because there are {} messages waiting to be ack'ed but limit is {}", getOutstandingMessagesToAck(), maxMessagesToAcknowledge);
			Thread.sleep(100);
		}
	}

	private Integer getOutstandingMessagesToAck() {
		return acknowledgeOnCheckpoint.numberOfOutstandingAcknowledgements();
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deserializationSchema.getProducedType();
	}

	public static <OUT> PubSubSourceBuilder<OUT> newBuilder(DeserializationSchema<OUT> deserializationSchema,
															String projectName,
															String subscriptionName) {
		Preconditions.checkNotNull(deserializationSchema);
		Preconditions.checkNotNull(projectName);
		Preconditions.checkNotNull(subscriptionName);
		return new PubSubSourceBuilder<>(new DeserializationSchemaWrapper<>(deserializationSchema), projectName, subscriptionName);
	}

	public static <OUT> PubSubSourceBuilder<OUT> newBuilder(PubSubDeserializationSchema<OUT> deserializationSchema,
															String projectName,
															String subscriptionName) {
		Preconditions.checkNotNull(deserializationSchema);
		Preconditions.checkNotNull(projectName);
		Preconditions.checkNotNull(subscriptionName);
		return new PubSubSourceBuilder<>(deserializationSchema, projectName, subscriptionName);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		acknowledgeOnCheckpoint.notifyCheckpointComplete(checkpointId);
	}

	@Override
	public List<AcknowledgeIdsForCheckpoint<String>> snapshotState(long checkpointId, long timestamp) throws Exception {
		return acknowledgeOnCheckpoint.snapshotState(checkpointId, timestamp);
	}

	@Override
	public void restoreState(List<AcknowledgeIdsForCheckpoint<String>> state) throws Exception {
		createAndSetAcknowledgeOnCheckpoint();
		acknowledgeOnCheckpoint.restoreState(state);
	}

	private void createAndSetAcknowledgeOnCheckpoint() {
		if (acknowledgeOnCheckpoint == null) {
			acknowledgeOnCheckpoint = acknowledgeOnCheckpointFactory.create(this);
		}
	}

	/**
	 * Builder to create PubSubSource.
	 *
	 * @param <OUT> The type of objects which will be read
	 */
	public static class PubSubSourceBuilder<OUT> {
		private final PubSubDeserializationSchema<OUT> deserializationSchema;
		private final String projectSubscriptionName;

		private PubSubSubscriberFactory pubSubSubscriberFactory;
		private Credentials credentials;
		private int maxMessageToAcknowledge = 10000;

		protected PubSubSourceBuilder(PubSubDeserializationSchema<OUT> deserializationSchema, String projectName, String subscriptionName) {
			this.deserializationSchema = deserializationSchema;
			this.projectSubscriptionName = ProjectSubscriptionName.format(projectName, subscriptionName);
		}

		/**
		 * Set the credentials.
		 * If this is not used then the credentials are picked up from the environment variables.
		 *
		 * @param credentials the Credentials needed to connect.
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withCredentials(Credentials credentials) {
			this.credentials = credentials;
			return this;
		}

		/**
		 * Set a PubSubSubscriberFactory
		 * This allows for custom Subscriber options to be set.
		 * {@link DefaultPubSubSubscriberFactory} is the default.
		 *
		 * @param pubSubSubscriberFactory A factory to create a {@link Subscriber}
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withPubSubSubscriberFactory(PubSubSubscriberFactory pubSubSubscriberFactory) {
			this.pubSubSubscriberFactory = pubSubSubscriberFactory;
			return this;
		}

		/**
		 * There is a default PubSubSubscriber factory that uses gRPC to pull in PubSub messages. This method can be used to tune this default factory.
         * Note this will not work in combination with a custom PubSubSubscriber factory.
		 * @param maxMessagesPerPull the number of messages pulled per request. Default: 100
		 * @param perRequestTimeout the timeout per request. Default: 15 seconds
		 * @param retries the number of retries when requests fail
		 * @return The current PubSubSourceBuilder instance
		 */
		public PubSubSourceBuilder<OUT> withPubSubSubscriberFactory(int maxMessagesPerPull, Duration perRequestTimeout, int retries) {
			this.pubSubSubscriberFactory = new DefaultPubSubSubscriberFactory(projectSubscriptionName, retries, perRequestTimeout, maxMessagesPerPull);
			return this;
		}

		/**
		 * Set a limit of the number of outstanding or to-be acknowledged messages.
		 * default is 10000. Adjust this if you have high checkpoint intervals and / or run into memory issues
		 * due to the amount of acknowledgement ids. Use {@link PubSubSource}.NO_MAX_MESSAGES_TO_ACKNOWLEDGE_LIMIT if you want to remove the limit.
		 */
		public PubSubSourceBuilder<OUT> withMaxMessageToAcknowledge(int maxMessageToAcknowledge) {
			this.maxMessageToAcknowledge = maxMessageToAcknowledge;
			return this;
		}

		/**
		 * Actually build the desired instance of the PubSubSourceBuilder.
		 *
		 * @return a brand new SourceFunction
		 * @throws IOException              in case of a problem getting the credentials
		 * @throws IllegalArgumentException in case required fields were not specified.
		 */
		public PubSubSource<OUT> build() throws IOException {
			if (credentials == null) {
				credentials = defaultCredentialsProviderBuilder().build().getCredentials();
			}

			if (pubSubSubscriberFactory == null) {
				pubSubSubscriberFactory = new DefaultPubSubSubscriberFactory(projectSubscriptionName, 3, Duration.ofSeconds(15), 100);
			}

			return new PubSubSource<>(deserializationSchema, pubSubSubscriberFactory, credentials, maxMessageToAcknowledge, new AcknowledgeOnCheckpointFactory());
		}
	}

	static class AcknowledgeOnCheckpointFactory implements Serializable {
		AcknowledgeOnCheckpoint<String> create(Acknowledger<String> acknowledger) {
			return new AcknowledgeOnCheckpoint<>(acknowledger);
		}
	}
}
