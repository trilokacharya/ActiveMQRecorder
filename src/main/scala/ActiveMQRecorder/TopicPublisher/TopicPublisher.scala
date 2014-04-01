package ActiveMQRecorder.TopicPublisher

import org.apache.activemq.{ActiveMQConnectionFactory, ActiveMQConnection}
import javax.jms._

/**
 * Created by trilok on 3/31/14.
 */
class TopicPublisher(val user:String,val password:String,val client:String,val subject:String,val url:String) {

  var DEFAULT_URL:String = ActiveMQConnection.DEFAULT_BROKER_URL

  var connection:Connection = null
  var session:Session = null
  var destination:Destination= null
  var producer:MessageProducer = null


  def StartConnection(){
    val connectionFactory:ActiveMQConnectionFactory= new ActiveMQConnectionFactory(user, password,url )
    connection= connectionFactory.createConnection()
    //connection.setClientID(client) - optional. Required for guaranteed delivery though
    connection.start()

    session = connection.createSession(false,Session.CLIENT_ACKNOWLEDGE)
    destination = session.createTopic(subject)

    producer = session.createProducer(destination)
    producer.setDeliveryMode(DeliveryMode.PERSISTENT)
  }

  /**
   * Publish a text message with the given string
   * @param msg
   */
  def PublishMessage(msg:String){
    val message = session.createTextMessage(msg)
    producer.send(destination,message)
  }

  def close(){
    if(producer!=null) try{producer.close()} catch { case _: Throwable =>  }
    if(session!=null) try{session.close()} catch { case _: Throwable =>   }
    if(connection!=null) try{connection.close()} catch { case _:Throwable =>   }
  }

}
