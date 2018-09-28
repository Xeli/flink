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

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.apache.flink.api.java.ClosureCleaner.ensureSerializable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SubscriberWrapper}.
 */
@RunWith(MockitoJUnitRunner.class)
public class SubscriberWrapperTest {
	@Mock
	private SerializableCredentialsProvider credentialsProvider;

	@Mock
	private MessageReceiver messageReceiver;

	@Mock
	private PubSubSubscriberFactory pubSubSubscriberFactory;

	@Mock
	private Subscriber subscriber;

	@Test
	public void testSerializedSubscriberBuilder() throws Exception {
		SubscriberWrapper factory = new SubscriberWrapper(SerializableCredentialsProvider.withoutCredentials(), ProjectSubscriptionName.of("projectId", "subscriptionId"), pubSubSubscriberFactory);
		ensureSerializable(factory);
	}

	@Test
	public void testInitialisation() {
		when(pubSubSubscriberFactory.getSubscriber(any(), any(), any())).thenReturn(subscriber);
		SubscriberWrapper factory = new SubscriberWrapper(credentialsProvider, ProjectSubscriptionName.of("projectId", "subscriptionId"), pubSubSubscriberFactory);
		factory.initialize(messageReceiver);

		assertThat(factory.getSubscriber(), is(subscriber));
	}
}
