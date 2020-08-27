import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._


/**
 * Description:
 *
 */
object TestFuture extends App {
  val startTime = currentTime

  val aaplFuture = getStockPrice("AAPL")
  val amznFuture = getStockPrice("AMZN")
  val googFuture = getStockPrice("GOOG")

  val result: Future[(Double, Double, Double)] = for {
    aapl <- aaplFuture
    amzn <- amznFuture
    goog <- googFuture
  } yield (aapl, amzn, goog)

  result.onComplete {
    case Success(x) => {
      val endTime = deltaTime(startTime)
      println(s"In Success case, time delta: ${endTime}")
      println(s"The stock prices are: ${x}")
    }
    case Failure(e) => e.printStackTrace()
  }

  println(s"wait start. ${System.currentTimeMillis()}")
  Await.result(Future { sleep(5000)}, 1 seconds)
  println(s"wait stop.  ${System.currentTimeMillis()}")

  def sleep(time: Long) = Thread.sleep(time)

  def getStockPrice(str: String): Future[Double] = Future {
    val r = scala.util.Random
    val randomSleepTime = r.nextInt(3000)
    println(s"For ${str}, sleep time is $randomSleepTime")
    val randomPrice = r.nextDouble() * 1000
    sleep(randomSleepTime)
    randomPrice
  }

  def currentTime = System.currentTimeMillis()
  def deltaTime(t0: Long) = currentTime - t0
}
