import arrow.Kind
import arrow.core.*
import arrow.core.extensions.list.traverse.sequence
import arrow.core.extensions.option.applicative.applicative
import arrow.core.extensions.option.applicative.map
import arrow.fx.*
import arrow.fx.extensions.fx
import arrow.fx.extensions.io.concurrent.concurrent
import arrow.fx.typeclasses.Concurrent
import arrow.fx.typeclasses.Duration
import arrow.fx.typeclasses.milliseconds
import arrow.fx.typeclasses.seconds
import io.hexlabs.concurrent.Queue

/**
 * Example usage of bounded queue built using Arrow Ref and Promise
 * Takers suspended when nothing to consume from Queue
 * Producers suspended when queue capacity reached
 */
fun main() {
  println(simpleOfferTake.unsafeRunSync()) // Tuple3(a=10, b=20, c=30)
  println("-----------")
  println(suspendOfferFiber.unsafeRunSync()) // 20
  println("-----------")
//  println(suspendOffer.unsafeRunSync()) // will not run
  println("-----------")
//  println(suspendTakers.unsafeRunSync()) // Tuple3(a=10, b=20, c=30)
  println("-----------")
  multiProducerMultiConsumer.unsafeRunSync()
//  producer 3 offering: 7
//  producer 1 offering: 10
//  consumer 1 taking 7
//  consumer 1 taking 10
//  producer 2 offering: 5
//  producer 1 offering: 10
//  producer 3 offering: 7
//  ...
  val l = listOf(Option(1), Option(2)).sequence(Option.applicative()).map { it.fix() }.fix()

  val nl = NonEmptyList.of(Option(1), Option(2)).sequence(Option.applicative()).fix()

}

fun <T> IO.Companion.boundedQueue(capacity: Int) =
  Queue.bounded<ForIO, T>(capacity, IO.concurrent())

fun putStrLn(s: String) = IO.later { println(s) }

fun <F, A, B> Kind<F, A>.repeatEvery(duration: Duration, C: Concurrent<F>): Kind<F, B> = C.run {
  // allocate two things once for efficiency.
  val leftUnit = { _: A -> Either.Left(Unit) }
  val stepResult: (Unit) -> Kind<F, Either<Unit, B>> = {
    Timer(C).sleep(duration).flatMap {
      this@repeatEvery.map(leftUnit)
    }
  }
  tailRecM(Unit, stepResult)
}

fun <A, B> IO<A>.repeatEvery(duration: Duration) = repeatEvery<ForIO, A, B>(duration, IO.concurrent()).fix()

// push data, consume, return result
val simpleOfferTake = IO.fx {
  val queue = !IO.boundedQueue<Int>(10)
  !queue.offer(10)
  !queue.offer(20)
  !queue.offer(30)
  val t1 = !queue.take()
  val t2 = !queue.take()
  val t3 = !queue.take()
  Tuple3(t1, t2, t3)
}

// bounded queue of 1, 2nd offer suspends in separate fiber until another take
// returns result of 2nd take
val suspendOfferFiber = IO.fx {
  val context = dispatchers().default()
  val queue = !IO.boundedQueue<Int>(1)
  !queue.offer(10)
  val f = !queue.offer(20).fork(context) // will be suspended because the queue is full
  !queue.take()
  // join fibre, suspended offer will have completed
  !f.join()
  !queue.take()
}

// same as suspendOfferFiber but will not complete as 2nd offer is not on separate fiber
val suspendOffer = IO.fx {
  val queue = !IO.boundedQueue<Int>(1)
  !queue.offer(10)
  !queue.offer(20) // will be suspended because the queue is full
  !putStrLn("attempt take: Code note reached")
  !queue.take() // never reached
}

// 3 take requests suspended waiting for offers to queue.
// 3 offers made and take results retrieved using Fiber#join
val suspendTakers = IO.fx {
  val context = dispatchers().default()
  // still a bounded queue of 1
  val queue = !IO.boundedQueue<Int>(1)

  // start many takers that will suspend on fibers
  val f1 = !queue.take().fork(context)
  val f2 = !queue.take().fork(context)
  val f3 = !queue.take().fork(context)
  // each offer completes a taker that consumes and allows the
  // next offer to proceed
  !queue.offer(10)
  !queue.offer(20)
  !queue.offer(30)

  val t1 = !f1.join()
  val t2 = !f2.join()
  val t3 = !f3.join()
  Tuple3(t1, t2, t3)
}


fun <A> offerAfter(latency: Duration, label: String, a: A, queue: Queue<ForIO, A>): IO<Unit> = IO.fx {
  !putStrLn("$label: $a")
  !queue.offer(a)
}.repeatEvery(latency)

fun <A> consumeAfter(latency: Duration, label: String, queue: Queue<ForIO, A>): IO<Unit> = IO.fx {
  val m = !queue.take()
  !putStrLn("$label $m")
}.repeatEvery(latency)

val multiProducerMultiConsumer = IO.fx {
  val context = dispatchers().default()
  val queue = !IO.boundedQueue<Int>(10000)
  val fo1 = !offerAfter(1.seconds, "producer 1 offering", 10, queue).fork(context)
  val fo2 = !offerAfter(2.seconds, "producer 2 offering", 5, queue).fork(context)
  val fo3 = !offerAfter(1.seconds, "producer 3 offering", 7, queue).fork(context)
  val fc1 = !consumeAfter(500.milliseconds, "consumer 1 taking", queue).fork(context)
  val fc2 = !consumeAfter(700.milliseconds, "consumer 2 taking", queue).fork(context)
  !timer().sleep(10.seconds)
  !fo1.cancel()
  !fo2.cancel()
  !fo3.cancel()
  !fc1.cancel()
  !fc2.cancel()
}
