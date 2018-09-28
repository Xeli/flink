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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wrapper class around a PubSub {@link Subscriber}.
 * This class makes it easier to connect to a Non Google PubSub service such as a local PubSub emulator or docker container.
 */
class SubscriberWrapper implements Serializable {
	private final SerializableCredentialsProvider serializableCredentialsProvider;
	private final String projectId;
	private final String subscriptionId;
	private final PubSubSubscriberFactory pubSubSubscriberFactory;

	private transient Subscriber subscriber;
	private volatile boolean running = true;

	SubscriberWrapper(SerializableCredentialsProvider serializableCredentialsProvider, ProjectSubscriptionName projectSubscriptionName, PubSubSubscriberFactory pubSubSubscriberFactory) {
		this.serializableCredentialsProvider = serializableCredentialsProvider;
		this.projectId = projectSubscriptionName.getProject();
		this.subscriptionId = projectSubscriptionName.getSubscription();
		this.pubSubSubscriberFactory = pubSubSubscriberFactory;
	}

	void initialize(MessageReceiver messageReceiver) {
		this.subscriber = pubSubSubscriberFactory.getSubscriber(serializableCredentialsProvider, ProjectSubscriptionName.of(projectId, subscriptionId), messageReceiver);
	}

	void startBlocking() {
		ApiService apiService = subscriber.startAsync();
		apiService.awaitRunning();

		if (apiService.state() != ApiService.State.RUNNING) {
			throw new IllegalStateException("Could not start PubSubSubscriber, ApiService.State: " + apiService.state());
		}
		while (running) {
			awaitTerminated(apiService, 5);
		}
	}

	void stop() {
		running = false;
		if (subscriber != null) {
			subscriber.stopAsync().awaitTerminated();
		}
	}

	private void awaitTerminated(ApiService apiService, long seconds) {
		try {
			apiService.awaitTerminated(seconds, TimeUnit.SECONDS);
		} catch (TimeoutException e) {}
	}

	Subscriber getSubscriber() {
		return subscriber;
	}
}
