package io.hexlabs.concurrent

import arrow.Kind
import arrow.core.*
import arrow.fx.Promise
import arrow.fx.Ref
import arrow.fx.typeclasses.Concurrent
import java.util.*
import java.util.Queue as JQueue

class Queue<F, A> private constructor(val capacity: Int, private val ref: Ref<F, State<F, A>>, C: Concurrent<F>) :
  Concurrent<F> by C {

  fun size(): Kind<F, Int> = ref.get().map { it.size() }

  fun offer(a: A): Kind<F, Unit> {
    val use: (Promise<F, Unit>, State<F, A>) -> Tuple2<Kind<F, Unit>, State<F, A>> = { p, state ->
      state.fold(
        {
          with(it) {
            if (queue.size < capacity && putters.isEmpty())
              p.complete(Unit) toT copy(queue = queue.enqueue(a))
            else
              unit() toT State.Surplus(queue, putters.enqueue(a toT p))
          }
        },
        {
          it.takers.dequeueOption().fold(
            { p.complete(Unit) toT State.Surplus(emptyJQueue<A>().enqueue(a), emptyJQueue()) },
            { (taker, takers) ->
              taker.complete(a) productR p.complete(Unit) toT State.Deficit(takers)
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
              { just(Unit) toT State.Deficit(emptyJQueue<Promise<F, A>>().enqueue(p)) },
              { (putter, putters) ->
                val (a, prom) = putter
                (prom.complete(Unit) productR p.complete(a)) toT State.Surplus(
                  emptyJQueue(),
                  putters
                )
              }
            )
          },
            { (a, q) ->
              surplus.putters.dequeueOption().fold(
                { p.complete(a) toT State.Surplus(q, surplus.putters) },
                { (putter, putters) ->
                  val (putVal, putProm) = putter
                  (putProm.complete(Unit) productR p.complete(a)) toT State.Surplus(q.enqueue(putVal), putters)
                })
            }
          )
        },
        { deficit -> just(Unit) toT State.Deficit(deficit.takers.enqueue(p)) }
      )
    }

    val release: (Unit, Promise<F, A>) -> Kind<F, Unit> =
      { _, p -> removeTaker(p) }
    return bracket(ref, use, release)
  }

  companion object {
    fun <F, A> bounded(capacity: Int, C: Concurrent<F>): Kind<F, Queue<F, A>> = C.run {
      ref<State<F, A>> { State.Surplus(emptyJQueue(), emptyJQueue()) }.map {
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
      !(pRef.update { Some(c toT pb) } productR just(pb)).uncancelable()
      !pb.get()
    }.guarantee(pRef.get().flatMap { it.map { (c, fb) -> release(c, fb) }.getOrElse { just(Unit) } })
      )
//    val state = !ref.get()
//    !later { println("current state $state") }
    res
  }

  private fun <A> removeTaker(taker: Promise<F, A>): Kind<F, Unit> =
    ref.update { state ->
      state.fold(::identity) { deficit -> State.Deficit(deficit.takers.filterNot { it == taker }) }
    }

  private fun removePutter(putter: Promise<F, Unit>): Kind<F, Unit> =
    ref.update { state ->
      state.fold(
        { surplus -> State.Surplus(surplus.queue, surplus.putters.filterNot { it == putter }) },
        ::identity
      )
    }

  sealed class State<F, A> {
    abstract fun size(): Int

    internal data class Deficit<F, A>(val takers: JQueue<Promise<F, A>>) : State<F, A>() {
      override fun size(): Int = -takers.size
    }

    internal data class Surplus<F, A>(val queue: JQueue<A>, val putters: JQueue<Tuple2<A, Promise<F, Unit>>>) :
      State<F, A>() {
      override fun size(): Int = queue.size + putters.size
    }
  }

  internal fun <F, A, C> State<F, A>.fold(
    ifSurplus: (State.Surplus<F, A>) -> C,
    ifDeficit: (State.Deficit<F, A>) -> C
  ) =
    when (this) {
      is State.Surplus -> ifSurplus(this)
      is State.Deficit -> ifDeficit(this)
    }

  infix fun <A, B> Kind<F, A>.productR(fb: Kind<F, B>): Kind<F, B> = map2(fb) { (_, b) -> b }
}

fun <A> JQueue<A>.filter(pred: (A) -> Boolean) =
  this.fold(emptyJQueue<A>()) { queue, elem ->
    if (pred(elem)) queue.enqueue(elem)
    else queue
  }

fun <A> JQueue<A>.filterNot(pred: (A) -> Boolean) =
  filter { !pred(it) }

fun <A> JQueue<A>.dequeue(): Tuple2<A, JQueue<A>>? =
  this.poll()?.let { it toT this }

fun <A> JQueue<A>.enqueue(a: A): JQueue<A> = apply { add(a) }

fun <A> JQueue<A>.dequeueOption(): Option<Tuple2<A, JQueue<A>>> =
  dequeue().toOption()

//    this.poll().toOption().map { it toT this }
fun <A, R> JQueue<A>.fold(r: R, f: (R, A) -> R): R =
  dequeue()?.let { (a, queue) ->
    queue.fold(f(r, a), f)
  } ?: r

fun <A> emptyJQueue(): JQueue<A> = LinkedList<A>()
