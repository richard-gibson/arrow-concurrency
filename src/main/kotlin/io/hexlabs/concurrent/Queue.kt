package io.hexlabs.concurrent

import arrow.Kind
import arrow.core.*
import arrow.fx.Promise
import arrow.fx.Ref
import arrow.fx.typeclasses.Concurrent
import arrow.fx.typeclasses.Fiber
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError

class Queue<F, A> private constructor(val capacity: Int, private val ref: Ref<F, State<F, A>>, val C: Concurrent<F>) :
  Concurrent<F> by C {

  fun size(): Kind<F, Int> = ref.get().flatMap { it.size() }

  fun offer(a: A): Kind<F, Unit> {
    val use: (Promise<F, Unit>, State<F, A>) -> Tuple2<Kind<F, Unit>, State<F, A>> = { p, state ->
      state.fold(
        ifSurplus = {
          it.run {
            if (queue.length() < capacity && putters.isEmpty())
              p.complete(Unit) toT copy(queue = queue.enqueue(a))
            else
              unit() toT it.copy(putters = putters.enqueue(a toT p))
          }
        },
        ifDeficit = {
          it.takers.dequeueOption().fold(
            { p.complete(Unit) toT State.Surplus(IQueue.empty<A>().enqueue(a), IQueue.empty(), C, it.shutdownHook) },
            { (taker, takers) ->
              taker.complete(a) zipRight p.complete(Unit) toT it.copy(takers = takers)
            }
          )
        },
        ifShutdown = { p.error(QueueShutdown) toT state }
      )
    }

    val release: (Unit, Promise<F, Unit>) -> Kind<F, Unit> =
      { _, p -> removePutter(p) }

    return bracket(ref, use, release)
  }

  fun take(): Kind<F, A> {
    val use: (Promise<F, A>, State<F, A>) -> Tuple2<Kind<F, Unit>, State<F, A>> = { p, state ->
      state.fold(
        ifSurplus = { surplus ->
          surplus.queue.dequeueOption().fold({
            surplus.putters.dequeueOption().fold(
              { just(Unit) toT State.Deficit(IQueue.empty<Promise<F, A>>().enqueue(p), C, surplus.shutdownHook) },
              { (putter, putters) ->
                val (a, prom) = putter
                (prom.complete(Unit) zipRight p.complete(a)) toT surplus.copy(
                  queue = IQueue.empty(),
                  putters = putters
                )
              }
            )
          },
            { (a, q) ->
              surplus.putters.dequeueOption().fold(
                { p.complete(a) toT surplus.copy(queue = q) },
                { (putter, putters) ->
                  val (putVal, putProm) = putter
                  (putProm.complete(Unit) zipRight p.complete(a)) toT surplus.copy(
                    queue = q.enqueue(putVal),
                    putters = putters
                  )
                })
            }
          )
        },
        ifDeficit = { deficit -> just(Unit) toT deficit.copy(takers = deficit.takers.enqueue(p)) },
        ifShutdown = { p.error(QueueShutdown) toT state }
      )
    }

    val release: (Unit, Promise<F, A>) -> Kind<F, Unit> =
      { _, p -> removeTaker(p) }
    return bracket(ref, use, release)
  }


  /**
   * Waits until the queue is shutdown.
   * The `IO` returned by this method will not resume until the queue has been shutdown.
   * If the queue is already shutdown, the `IO` will resume right away.
   */
  fun awaitShutdown() : Kind<F, Unit> =
    Promise<F, Unit>(C).flatMap { promise ->
        val io = promise.complete(Unit)
        ref.modify { state ->
          state.fold(
            ifSurplus = { it.copy(shutdownHook = it.shutdownHook zipRight io.unit()) toT unit() },
            ifDeficit = { it.copy(shutdownHook = it.shutdownHook zipRight io.unit()) toT unit() },
            ifShutdown = { state toT io.unit() }
          )
        }.flatten() zipRight promise.get()
    }

  /**
   * Cancels any fibers that are suspended on `offer` or `take`.
   * Future calls to `offer*` and `take*` will be interrupted immediately.
   */
 /* fun shutdown() : Kind<F, Unit> =  ref.modify { state ->
    state.fold(
      ifSurplus = {
        if(it.putters.isEmpty())
          Companion.State.Shutdown(C) toT it.shutdownHook
        else
          it toT it.putters.toList().k().traverse(C) { (_, p) -> p.error(QueueShutdown) }
            //.traverse(Fiber).map {  }
         },
      ifDeficit = { it.copy(shutdownHook = it.shutdownHook zipRight unit()) toT unit() },
      ifShutdown = { state toT unit() }
    )
  }.flatten()*/


  //TODO: correct the context to do this
  fun Concurrent<F>.forkAll(iter: Iterable<Kind<F, A>>): Kind<F, Fiber<F, List<A>>> {

    val initial = just(just(emptyList<A>()).fork())
    iter.fold(initial) { acc, elem ->
      acc.map { it + listOf(elem.fork()) }

    }
    return TODO()
  }




/*  */
 /*
    final def forkAll[E, A](as: Iterable[IO[E, A]]): IO[Nothing, Fiber[E, List[A]]] =
    as.foldRight(IO.point(Fiber.point[E, List[A]](List()))) { (aIO, asFiberIO) =>
      asFiberIO.seq(aIO.fork).map {
        case (asFiber, aFiber) =>
          asFiber.zipWith(aFiber)((as, a) => a :: as)
      }
    }
*/

  companion object {
    fun <F, A> bounded(capacity: Int, C: Concurrent<F>): Kind<F, Queue<F, A>> = C.run {
      Ref<State<F, A>>(State.Surplus(IQueue.empty(), IQueue.empty(), this, unit())).map {
        Queue(capacity, it, this)
      }
    }

    internal sealed class State<F, out A> {
      abstract fun size(): Kind<F, Int>

      internal data class Deficit<F, A>(val takers: IQueue<Promise<F, A>>,
                                        val AP: Applicative<F>,
                                        val shutdownHook: Kind<F, Unit>) : State<F, A>() {
        override fun size(): Kind<F, Int> = AP.just(-takers.length())
      }

      internal data class Surplus<F, A>(val queue: IQueue<A>,
                                        val putters: IQueue<Tuple2<A, Promise<F, Unit>>>,
                                        val AP: Applicative<F>,
                                        val shutdownHook: Kind<F, Unit>) : State<F, A>() {
        override fun size(): Kind<F, Int> = AP.just(queue.length() + putters.length())
      }

      internal data class Shutdown<F>(val AE: ApplicativeError<F, Throwable>) : State<F, Nothing>() {
        override fun size(): Kind<F, Int> = AE.raiseError(QueueShutdown)
      }
    }

    internal fun <F, A, C> State<F, A>.fold(
      ifSurplus: (State.Surplus<F, A>) -> C,
      ifDeficit: (State.Deficit<F, A>) -> C,
      ifShutdown: (State.Shutdown<F>) -> C = TODO()
    ) =
      when (this) {
        is State.Surplus -> ifSurplus(this)
        is State.Deficit -> ifDeficit(this)
        is State.Shutdown -> ifShutdown(this)
      }

  }

  private fun <A, B, C> bracket(
    ref: Ref<F, A>,
    use: (Promise<F, B>, A) -> Tuple2<Kind<F, C>, A>,
    release: (C, Promise<F, B>) -> Kind<F, Unit>
  ): Kind<F, B> = fx.concurrent {
    val pRef = !Ref<Option<Tuple2<C, Promise<F, B>>>>(None)
    val res = (!fx.concurrent {

      // creates a new promise for `use` and returns
      val (fc, pb) = !ref.modify { a ->
        val pb = Promise.unsafeCancelable<F, B>(this)
        val (fc, a2) = use(pb, a)
        a2 toT (fc toT pb)
      }
      val c = !fc
      !(pRef.set(Some(c toT pb)) zipRight just(pb)).uncancelable()
      !pb.get()
    }.guarantee(pRef.get().flatMap { it.map { (c, fb) -> release(c, fb) }.getOrElse { just(Unit) } })
      )
//    val state = !ref.get()
//    !later { println("current state $state") }
    res
  }

  private fun <A> removeTaker(taker: Promise<F, A>): Kind<F, Unit> =
    ref.update { state ->
      state.fold(ifSurplus = ::identity,
        ifDeficit = {
          it.run { copy(takers.filterNot { t -> t == taker }) }
        })
    }

  private fun removePutter(putter: Promise<F, Unit>): Kind<F, Unit> =
    ref.update { state ->
      state.fold(
        ifSurplus = {
          it.run { copy(putters = putters.filterNot { p -> p == putter }) }
        },
        ifDeficit = ::identity
      )
    }

  infix fun <A, B> Kind<F, A>.zipRight(fb: Kind<F, B>): Kind<F, B> = map2(fb) { (_, b) -> b }
}


object QueueShutdown : RuntimeException() {
  override fun fillInStackTrace(): Throwable = this
}
