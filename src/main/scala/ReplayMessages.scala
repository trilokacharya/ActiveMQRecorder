/**
 * Created by trilok on 3/26/14.
 */
package messageReplay

import com.github.nscala_time.time.Imports._
import org.json4s.native._
import org.json4s._
import java.io.RandomAccessFile
import org.json4s.JValue
import scala.annotation.tailrec


class ReplayMessages(val threshold:Long=10000) {

  implicit val formats = org.json4s.DefaultFormats // required for extracting object out of JSON
  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val magicString = "%^#"

  def parsedMessage(msg:JValue):MsgFormat= msg.extract[MsgFormat]

  /**
   * Read a line from a RandomAccessFile object and also return it's original file pointer position
   * @param raf
   * @return
   */
  def readLineAndOldPos(raf:RandomAccessFile):(Option[String],Long)={
    val originalPos= raf.getFilePointer
    val line=raf.readLine()
    //raf.seek(originalPos)
    if(line==null) (None,originalPos)
    else (Some(line),originalPos)
  }


  /**
   * Reads through the file starting at the given start position and tries to find the position of the timestamp that is
   * >= given timestamp
   * @param startPos
   * @param date
   * @param raf
   * @return
   */
  @tailrec
  final def sequentialSearchIter(startPos:Long, date:DateTime, raf:RandomAccessFile):Iterator[MsgFormat] ={
    raf.seek(startPos)
    if(startPos >= raf.length) Iterator[MsgFormat]()
    else{
      val msgOption = getNextMsg(raf)
      msgOption match{
        case None=> Iterator[MsgFormat]()
        case Some(msg)=> {
          val parsed:MsgFormat=parsedMessage(msg)
          if(parsed.parsedDate >= date) {
            Iterator(parsed) ++ new MsgIterator(raf)
          }
          else{
            sequentialSearchIter(raf.getFilePointer,date,raf)}
        }
      }
    }
  }


  /**
   * Reads through the file starting at the given start position and tries to find the position of the timestamp that is
   * >= given timestamp
   * @param startPos
   * @param date
   * @param raf
   * @return
   */
  @tailrec
  final def sequentialSearchStream(startPos:Long, date:DateTime, raf:RandomAccessFile):Stream[MsgFormat] ={
    raf.seek(startPos)
    if(startPos >= raf.length) Stream.Empty
    else{
      //val currPos = raf.getFilePointer
      val msgOption = getNextMsg(raf)
        msgOption match{
          case None=> Stream.Empty
          case Some(msg)=> {
            val parsed:MsgFormat=parsedMessage(msg)
            if(parsed.parsedDate >= date) {
              //raf.seek(currPos)
              //Some(raf)
              parsed#::getMsgStream(raf)
            }
            else{
            sequentialSearchStream(raf.getFilePointer,date,raf)}
          }
        }
      }
  }

  /**
   *Return a stream of messages starting from the given file pointer
   */
  def getMsgStream(raf:RandomAccessFile):Stream[MsgFormat]={
    getNextMsg(raf) match {
      case None => Stream.Empty
      case Some(msg) => parsedMessage(msg) #:: getMsgStream(raf)
    }
  }

  /**
   * Given a date and a file, returns the position in the file which has a message with date >= given date
   * @param date
   * @param raf
   * @return
   */
  def seekDateTimeIter(date:DateTime,raf:RandomAccessFile):Iterator[MsgFormat]=
  {
    raf.seek(0)
    val msg= getNextMsg(raf)
    if(msg.isDefined){
      val parsed= parsedMessage(msg.get)
      raf.seek(0)
      if(parsed.parsedDate>=date){ //if the very beginning of the file is already past our start timestamp
        new MsgIterator(raf)
      }
      else{
        seekDateTimeIter(0,raf.length(),date,raf)
      }
    }
    else{ // No more messages
      Iterator[MsgFormat]()
    }
  }

  /**
   *Find the position of the message that starts at the specified date, within the startPos and endPos boundaries
   */
  def seekDateTimeIter(startPos:Long,endPos:Long,date:DateTime,raf:RandomAccessFile):Iterator[MsgFormat]={
    if(endPos<=startPos) Iterator[MsgFormat]() // can't find the startdate
    else if(endPos-startPos <= threshold) sequentialSearchIter(startPos,date,raf)
    else{
      val midPos=(startPos+endPos)/2
      raf.seek(midPos)
      val msg = getNextMsg(raf)
      if(msg.isDefined){
        val parsed=parsedMessage(msg.get)
        if(parsed.parsedDate<date){
          seekDateTimeIter(midPos,endPos,date,raf)
        }else if(parsed.parsedDate==date){
          Iterator(parsed) ++ new MsgIterator(raf)
          //parsed #:: getMsgStream(raf)
        }
        else{
          seekDateTimeIter(startPos,midPos,date,raf)
        }
      }
      else Iterator[MsgFormat]()
    }
  }

  /**
   * Given a date and a file, returns the position in the file which has a message with date >= given date
   * @param date
   * @param raf
   * @return
   */
  def seekDateTimeStream(date:DateTime,raf:RandomAccessFile):Stream[MsgFormat]=
  {
    raf.seek(0)
    val msg= getNextMsg(raf)
    if(msg.isDefined){
      val parsed= parsedMessage(msg.get)
      raf.seek(0)
      if(parsed.parsedDate>=date){ //if the very beginning of the file is already past our start timestamp
        //Some(raf)
        getMsgStream(raf)
      }
      else{
        seekDateTimeStream(0,raf.length(),date,raf)
      }
    }
    else{ // No more messages
      Stream.Empty
    }
  }


  // Find the position of the message that starts at the specified date, within the startPos and endPos boundaries
  def seekDateTimeStream(startPos:Long,endPos:Long,date:DateTime,raf:RandomAccessFile):Stream[MsgFormat]={
    if(endPos<=startPos) Stream.Empty // can't find the startdate
    else if(endPos-startPos <= threshold) sequentialSearchStream(startPos,date,raf)
    else{
      val midPos=(startPos+endPos)/2
      raf.seek(midPos)
      val msg = getNextMsg(raf)
      if(msg.isDefined){
        val parsed=parsedMessage(msg.get)
        if(parsed.parsedDate<date){
          seekDateTimeStream(midPos,endPos,date,raf)
        }else if(parsed.parsedDate==date){
          //Some(raf)
          parsed #:: getMsgStream(raf)
        }
        else{
          seekDateTimeStream(startPos,midPos,date,raf)
        }
      }
      else Stream.Empty
    }
  }

  /**
   * Return the deserialized JSON value from the given string
   * @param line
   * @return
   */
  def parseLine(line:String):Option[JValue]=
    Some(parseJson(line.substring(magicString.length)))

  /**
   * Given a RandomAccessFile object, gets the next message from the file, starting from any arbitrary position.
   * Puts the RandomAccessFile pointer at the beginning of the line and returns the JSON value of that line
   * @param raf
   * @return
   */
  def getNextMsg(raf:RandomAccessFile):Option[JValue]={
    //var currRafPos = raf.getFilePointer
    var line = raf.readLine()
    if(line.startsWith(magicString)){
      //raf.seek(currRafPos) //This is where the line starts
      parseLine(line)
    }
    else{
      if(raf.getFilePointer < raf.length) { // within file boundary
      //  currRafPos=raf.getFilePointer
        line = raf.readLine()
     //   raf.seek(currRafPos) // put it back at the beginning of the line
        if(!line.startsWith(magicString)) throw new Exception("Invalid line. Doesn't start with "+magicString)
        parseLine(line)
      }
      else None
    }
  }

  /**
   * Date Comparison between a DateTime object and a String in the expected datetime format
   * @param dtStr
   * @param date
   * @return
   */
  def compareDate(dtStr:String,date:DateTime ):Int={
    val parsedDate=dateFormatter.parseDateTime(dtStr)
    if(parsedDate<date) -1
    else if(parsedDate>date) 1
    else 0 // equal
  }


  class MsgIterator(raf:RandomAccessFile) extends Iterator[MsgFormat]{
    override def next(): MsgFormat = {
      getNextMsg(raf) match {
        case None => throw new NoSuchElementException
        case Some(msg) => parsedMessage(msg)
      }
    }

    var nextMsg:Option[MsgFormat] = None

    override def hasNext: Boolean = {
      val rafPos = raf.getFilePointer
      val nxtMsg = getNextMsg(raf)
      raf.seek(rafPos) // reset position // reset position // reset position // reset position
      nxtMsg match {
        case None => false
        case _ => true
      }
    }
  }
}
