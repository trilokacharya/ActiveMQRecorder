/**
 * Created by tacharya on 2/20/14.
 */
//package SubTopic

import javax.jms.{Connection,Session}
import rx.lang.scala._
import java.io._
import resource._

object TopicSubscriberUser extends App {
  val user:String="user"
  val password:String="password123"
  val subject:String= "TA.IMQ.PUBLISH.MEMINFO"
  val client:String=""
  val url="tcp://localhost:61616"


// Use automatic resource management in the ARM library to write messages out to a files
  for( writer <- managed(new PrintWriter(new File("ActiveMQLog.txt")));
     subscriber <- managed(TopicSubscriber(user,password,client,subject,url)))
  {

    // get an observable
    val observable:Observable[String]=subscriber.CreateConnectionObservable

    // Make observables more like linQ by calling toBlockingObservable. No need to specify the onNext and onComplete
    // method in this case.
    observable.toBlockingObservable
      .withFilter(s=>s.contains("a test")) // keep only messages containing the phrase "a test"
      .foreach(x=>{         // foreach ends when onComplete is called.
                println("Observed "+x);
                writer.println(x)
        })

    // alternatively, we could use the subscribe method and specify onNext, onError and onComplete methods explicitly
    // However, this is not a blocking call, so now you need to have the main thread wait on some sort of signal that
    // is fired when we're done receiving all messages (when onComplete is called for example)
    observable.subscribe(
      (t:String) => { println("Observed "+t)},
      (e:Throwable) => {println("Error:"+e.getMessage)},
      ()=>{println("Done!")}
    )
  }

}

