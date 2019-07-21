package io.hexlabs.concurrent

import arrow.Kind
import arrow.core.*
import arrow.fx.Promise
import arrow.fx.Ref
import arrow.fx.typeclasses.Concurrent
import arrow.typeclasses.Applicative

class Queue<F, A> private constructor(val capacity: Int, val ref: Ref<F, State<F, A>>, val C: Concurrent<F>) :
  Concurrent<F> by C {

  fun size(): Kind<F, Int> = ref.get().flatMap { it.size() }

  fun offer(a: A): Kind<F, Unit> {
    val use: (Promise<F, Unit>, State<F, A>) -> Tuple2<Kind<F, Unit>, State<F, A>> = { p, state ->
      state.fold(
        {
          it.run {
            if (queue.length() < capacity && putters.isEmpty())
              p.complete(Unit) toT copy(queue = queue.enqueue(a))
            else
              unit() toT it.copy(putters = putters.enqueue(a toT p))
          }
        },
        {
          it.takers.dequeueOption().fold(
            { p.complete(Unit) toT State.Surplus(IQueue.empty<A>().enqueue(a), IQueue.empty(), C) },
            { (taker, takers) ->
              taker.complete(a) zipRight p.complete(Unit) toT it.copy(takers = takers)
            }
          )
        }
      )
    }

    val release: (Unit, Promise<F, Unit>) -> Kind<F, Unit> =
      { _, p -> removePutter(p) }

    return bracket(ref, use, release)
  }

  fun take(): Kind<F, A> {
    val use: (Promise<F, A>, State<F, A>) -> Tuple2<Kind<F, Unit>, State<F, A>> = { p, state ->
      state.fold(
        { surplus ->
          surplus.queue.dequeueOption().fold({
            surplus.putters.dequeueOption().fold(
              { just(Unit) toT State.Deficit(IQueue.empty<Promise<F, A>>().enqueue(p), C) },
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
        { deficit -> just(Unit) toT deficit.copy(takers = deficit.takers.enqueue(p)) }
      )
    }

    val release: (Unit, Promise<F, A>) -> Kind<F, Unit> =
      { _, p -> removeTaker(p) }
    return bracket(ref, use, release)
  }

  companion object {
    fun <F, A> bounded(capacity: Int, C: Concurrent<F>): Kind<F, Queue<F, A>> = C.run {
      ref<State<F, A>> { State.Surplus(IQueue.empty(), IQueue.empty(), C) }.map {
        Queue(capacity, it, this)
      }
    }
  }

  private fun <A, B, C> bracket(
    ref: Ref<F, A>,
    use: (Promise<F, B>, A) -> Tuple2<Kind<F, C>, A>,
    release: (C, Promise<F, B>) -> Kind<F, Unit>
  ): Kind<F, B> = fx.concurrent {

    val pRef = !ref<Option<Tuple2<C, Promise<F, B>>>> { None }
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
      state.fold(::identity) {
        it.run { copy(takers.filterNot { t -> t == taker }) }
      }
    }

  private fun removePutter(putter: Promise<F, Unit>): Kind<F, Unit> =
    ref.update { state ->
      state.fold(
        {
          it.run { copy(putters = putters.filterNot { p -> p == putter }) }
        },
        ::identity
      )
    }

  sealed class State<F, A> {
    abstract fun size(): Kind<F, Int>

    internal data class Deficit<F, A>(val takers: IQueue<Promise<F, A>>, val AP: Applicative<F>) : State<F, A>() {
      override fun size(): Kind<F, Int> = AP.just(-takers.length())
    }

    internal data class Surplus<F, A>(val queue: IQueue<A>, val putters: IQueue<Tuple2<A, Promise<F, Unit>>>, val AP: Applicative<F>) :
      State<F, A>() {
      override fun size(): Kind<F, Int> = AP.just(queue.length() + putters.length())
    }

/*    internal data class Shutdown<F>(val AE: ApplicativeError<F, Throwable>) : State<F, Nothing>() {
      override fun size(): Kind<F, Int> = AE.raiseError(QueueShutdown)
    }*/
  }

  internal fun <F, A, C> State<F, A>.fold(
    ifSurplus: (State.Surplus<F, A>) -> C,
    ifDeficit: (State.Deficit<F, A>) -> C
//    ifShutdown: (State.Shutdown<F>) -> C = TODO()
  ) =
    when (this) {
      is State.Surplus -> ifSurplus(this)
      is State.Deficit -> ifDeficit(this)
//      is State.Shutdown -> ifShutdown(this)
    }

  infix fun <A, B> Kind<F, A>.zipRight(fb: Kind<F, B>): Kind<F, B> = map2(fb) { (_, b) -> b }
}
