package PgnParser

import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.cloud.pubsub.v1.MessageReceiver
object PubSubSubscriber {
  def getSubscriber(messageReceiver: MessageReceiver) =
    val subscriptionName =
      ProjectSubscriptionName.of("greg-finley", "lichess-file-sub1")
    Subscriber.newBuilder(subscriptionName, messageReceiver).build()
}
