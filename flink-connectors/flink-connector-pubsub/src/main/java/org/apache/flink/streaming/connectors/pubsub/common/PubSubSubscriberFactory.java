package org.apache.flink.streaming.connectors.pubsub.common;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.io.Serializable;

/**
 * A factory class to create a {@link Subscriber}.
 * This allows for customized Subscribers with for instance tweaked configurations
 * Note: this class needs to be serializable.
 */
public interface PubSubSubscriberFactory extends Serializable {
	Subscriber getSubscriber(CredentialsProvider credentialsProvider, ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver);
}
