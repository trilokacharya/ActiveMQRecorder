/**
 * Created by tacharya on 2/20/14.
 */
package SubTopic

import javax.jms.{Connection,Session}
import rx.lang.scala._

object TopicSubscribe extends App {
  val user:String="user"
  val password:String="password123"
  val subject:String= "TA.IMQ.PUBLISH.MEMINFO"
  val client:String=""
  val url="tcp://localhost:61616"

  var subscriber:TopicSubscriber=null
  try
  {
    subscriber = TopicSubscriber(user,password,client,subject,url)
    val observable:Observable[String]=subscriber.CreateConnectionObservable
    observable.toBlockingObservable
      .withFilter(s=>s.contains("a test"))
      .foreach(x=>{println("Observed "+x)})
  }finally
  {
    if(subscriber!=null) subscriber.close()
  }

}

