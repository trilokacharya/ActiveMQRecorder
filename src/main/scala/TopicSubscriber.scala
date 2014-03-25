/**
 * Created by tacharya on 2/19/14.
 */

package SubTopic

import javax.jms._
import org.apache.activemq.{ActiveMQConnection, ActiveMQConnectionFactory}
import rx.lang.scala._
import java.lang.Throwable

object TopicSubscriber // (val user:String,val password:String,val client:String,val subject:String,val url:String)
{
  def apply(user:String,password:String,client:String,subject:String,url:String=ActiveMQConnection.DEFAULT_BROKER_URL) =
  {
    new TopicSubscriber(user,password,client,subject,url)
  }

}

/**
 * Class that subscribes to the given topic
 * @param user
 * @param password
 * @param client
 * @param subject
 * @param url
 */
class TopicSubscriber(val user:String,val password:String,val client:String,val subject:String,val url:String)
{
  var DEFAULT_URL:String = ActiveMQConnection.DEFAULT_BROKER_URL;

  var connection:Connection = null
  var session:Session = null

  var consumer:MessageConsumer=null

  // Takes Callback OnMessage, returns a connection and session as a tuple
  def CreateConnectionObservable:Observable[String]={
    val connectionFactory:ActiveMQConnectionFactory= new ActiveMQConnectionFactory(user, password,url )
    connection= connectionFactory.createConnection()
    //connection.setClientID(client)
    connection.start()

    session = connection.createSession(false,Session.CLIENT_ACKNOWLEDGE)
    consumer = session.createConsumer(session.createTopic(subject))

    // The Message Listner gets the Observer so that it can invoke the onNext method of the Observer when a new message
    // is received
    val observable:Observable[String]=Observable(
      observer =>{
        consumer.setMessageListener(new ListenAndObserve(observer))
        Subscription{}
      }
    )

    observable
  }


  /**
   * Close Consumer, Session and Connection. Ignore errors
   */
  def close()={
    if(consumer!=null) try{consumer.close()} catch { case _ =>   }
    if(session!=null) try{session.close()} catch { case _ =>   }
    if(connection!=null) try{connection.close()} catch { case _ =>   }
  }
}

class ListenAndObserve(val observer:Observer[String]) extends MessageListener {
  /**
   * When a new message is received, call OnNext of the observer with the message. Assuming it's a TextMessage for now
   * @param msg
   */
  @Override
  def onMessage(msg:Message):Unit ={
    val messageText =msg.asInstanceOf[TextMessage].getText()
    //println(messageText)
    observer.onNext(messageText)
  }
}

