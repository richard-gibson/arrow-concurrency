
import arrow.core.Tuple3
import arrow.fx.*
import arrow.fx.extensions.fx
import arrow.fx.extensions.io.concurrent.concurrent
import io.hexlabs.concurrent.Queue

fun <T> IO.Companion.boundedQueue(capacity: Int) =
  Queue.bounded<ForIO, T>(capacity, IO.concurrent())

fun <T> boundedQueueOf(capacity: Int) =
  Queue.bounded<ForIO, T>(capacity, IO.concurrent())


fun putStrLn(s: String) = IO.later { println(s) }

val res1 = IO.fx {
 val queue = !boundedQueueOf<Int>(10)
  !queue.offer(10)
  !queue.offer(20)
  !queue.offer(30)
  val a = !queue.take()
  val b = !queue.take()
  val c = !queue.take()
  Tuple3(a,b,c)
}

val res2 = IO.fx {
  val context = dispatchers().default()
  val queue = !boundedQueueOf<Int>(1)
  !putStrLn("offering: 10")
  queue.offer(10)
  !putStrLn("offering: 20 in fibre")
  val f = !context.startFiber(queue.offer(20))
  !putStrLn("taking from queue")
  !queue.take()
  // suspend until offering 20 completed
  !f.join()
  !putStrLn("offering 20 completed")
  !putStrLn("taking from queue")
  !queue.take()
}

val res2Suspended = IO.fx {
  val queue = !boundedQueueOf<Int>(1)
  !putStrLn("offering: 10")
  !queue.offer(10)
  !putStrLn("offering: 20")
  !queue.offer(20)
  !putStrLn("offering 20 completed")
  !putStrLn("taking from queue") //will not be reached
  !queue.take()

}

fun main() {
  println(res1.unsafeRunSync())

  println(res2.unsafeRunSync())
//  println(res2Suspended.unsafeRunSync())

}
