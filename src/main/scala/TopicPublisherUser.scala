/**
 * Created by tacharya on 4/1/14.
 */

import ActiveMQRecorder.MessageReplay.FindMessages
import ActiveMQRecorder.TopicPublisher.TopicPublisher
import java.io.RandomAccessFile
import ActiveMQRecorder.TopicPublisher
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import resource._

object TopicPublisherUser extends App{
  val user:String="user"
  val password:String="password123"
  val subject:String= "TA.IMQ.PUBLISH.MEMINFO"
  val client:String=""
  val url="tcp://localhost:61616"
  val dtFormat="yyyy-MM-dd HH:mm:ss"

  def PlaybackMessages(fileName:String, startTimeStamp:DateTime)={
    val raf = new RandomAccessFile(fileName,"r")
    val msgFinder = new FindMessages()
    val msgIterator=msgFinder.seekDateTimeIter(startTimeStamp,raf)

    for( topicPublisher <- managed( new TopicPublisher(user,password,client,subject,url))){
      topicPublisher.StartConnection()
      msgIterator.foreach(msg=>topicPublisher.PublishMessage(msg.payload))
    }
  }

  val filePath=getClass.getResource("/ActiveMQLog.txt").getPath
  var toFind="2014-03-28 19:13:12"
  val dFormat:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  var dateToFind =  DateTime.parse(toFind,dFormat)
  PlaybackMessages(filePath,dateToFind)

}
