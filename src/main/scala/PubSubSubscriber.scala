package PgnParser

import com.google.cloud.pubsub.v1.{MessageReceiver, Subscriber}
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.api.gax.batching.FlowControlSettings;

object PubSubSubscriber {
  def getSubscriber(messageReceiver: MessageReceiver) =
    val subscriptionName =
      ProjectSubscriptionName.of("greg-finley", "lichess-file-sub1")
    Subscriber
      .newBuilder(subscriptionName, messageReceiver)
      .setFlowControlSettings(
        FlowControlSettings
          .newBuilder()
          .setMaxOutstandingElementCount(1)
          .build()
      )
      .build()
}
