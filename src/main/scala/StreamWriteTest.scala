/**
 * Created by tacharya on 2/18/14.
 */
import java.io._
import rx.lang.scala._

object StreamWriteTest extends App{

  val test:Int = 5

  def testRand:Observable[Int] =
      Observable(
        observer => {
          for(i<- 1 until 2)
          {
            observer.onNext(i)
            Thread.sleep(5000);
          }
          Thread.sleep(20000);
          observer.onCompleted()
          Subscription{}
        })

  testRand.toBlockingObservable.foreach(i=>println(i))
  testRand.subscribe(
    (t:Int) => { println(t)},
    (e:Throwable) => {println(e.getMessage)},
      ()=>{println("Done!")}
  )


  println("Done subscribing")

  trait jpt{
    def bla():Unit
    val xo:Int
    def boo(b:Int):Unit
  }


  def taker(hmm:jpt):Unit={
    hmm.bla
    println(hmm.xo)
    hmm.boo(55)
  }

  taker( new jpt{
    def bla()={println("Bla ayo")}
    val xo=55
    def boo(t:Int)={println("int ayo:"+t.toString)}
    })

}


