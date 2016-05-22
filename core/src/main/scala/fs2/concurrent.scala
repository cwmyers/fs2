package fs2

import _root_.scalaz.concurrent.Task
import _root_.scalaz.stream.Process


object concurrent {

  /**
    * Nondeterministically merges a stream of streams (`outer`) in to a single stream,
    * opening at most `maxOpen` streams at any point in time.
    *
    * The outer stream is evaluated and each resulting inner stream is run concurrently,
    * up to `maxOpen` stream. Once this limit is reached, evaluation of the outer stream
    * is paused until one or more inner streams finish evaluating.
    *
    * When the outer stream stops gracefully, all inner streams continue to run,
    * resulting in a stream that will stop when all inner streams finish
    * their evaluation.
    *
    * When the outer stream fails, evaluation of all inner streams is interrupted
    * and the resulting stream will fail with same failure.
    *
    * When any of the inner streams fail, then the outer stream and all other inner
    * streams are interrupted, resulting in stream that fails with the error of the
    * stream that cased initial failure.
    *
    * Finalizers on each inner stream are run at the end of the inner stream,
    * concurrently with other stream computations.
    *
    * Finalizers on the outer stream are run after all inner streams have been pulled
    * from the outer stream -- hence, finalizers on the outer stream will likely run
    * before the last finalizer on the last inner stream.
    *
    * Finalizers on the returned stream are run after the outer stream has finished
    * and all open inner streams have finished.
    *
    * @param maxOpen    Maximum number of open inner streams at any time. Must be > 0.
    * @param outer      Stream of streams to join.
    */
  def join[F[_],O](maxOpen: Int)(outer: Stream[F,Stream[F,O]])(implicit F: Async[F]): Stream[F,O] = {
    assert(maxOpen > 0,"maxOpen must be > 0, was: " + maxOpen)

    def throttle[A](checkIfKilled: F[Boolean]): Pipe[F,Stream[F,A],Unit] = {

      def runInnerStream(inner: Stream[F,A], onInnerStreamDone: F[Unit]): Pull[F,Nothing,Unit] = {
        val startInnerStream: F[F.Ref[Unit]] = {
          F.bind(F.ref[Unit]) { gate =>
          F.map(F.start(
            Stream.eval(checkIfKilled).
                   flatMap { killed => if (killed) Stream.empty else inner }.
                   onFinalize { F.bind(F.setPure(gate)(())) { _ => onInnerStreamDone } }.
                   run
          )) { _ => gate }}
        }
        Pull.acquire(startInnerStream) { gate => F.get(gate) }.map { _ => () }
      }

      def go(doneQueue: async.mutable.Queue[F,Unit])(open: Int): (Stream.Handle[F,Stream[F,A]], Stream.Handle[F,Unit]) => Pull[F,Nothing,Unit] = (h, d) => {
        if (open < maxOpen)
          Pull.receive1Option[F,Stream[F,A],Nothing,Unit] {
            case Some(inner #: h) => runInnerStream(inner, F.map(F.start(doneQueue.enqueue1(())))(_ => ())).flatMap { gate => go(doneQueue)(open + 1)(h, d) }
            case None => Pull.done
          }(h)
        else
          d.receive1 { case _ #: d => go(doneQueue)(open - 1)(h, d) }
      }

      in => Stream.eval(async.unboundedQueue[F,Unit]).flatMap { doneQueue => in.pull2(doneQueue.dequeue)(go(doneQueue)(0)) }
    }

    for {
      killSignal <- Stream.eval(async.signalOf(false))
      outputQueue <- Stream.eval(async.mutable.Queue.synchronousNoneTerminated[F,Either[Throwable,Chunk[O]]])
      o <- outer.map { inner =>
        inner.chunks.attempt.evalMap { o => outputQueue.enqueue1(Some(o)) }.interruptWhen(killSignal)
      }.through(throttle(killSignal.get)).onFinalize {
        outputQueue.enqueue1(None)
      }.mergeDrainL {
        outputQueue.dequeue.through(pipe.unNoneTerminate).flatMap {
          case Left(e) => Stream.eval(killSignal.set(true)).flatMap { _ => Stream.fail(e) }
          case Right(c) => Stream.chunk(c)
        }
      }.onFinalize { killSignal.set(true) }
    } yield o
  }

  /**
    * A supply of `Long` values, starting with `initial`.
    * Each read is guaranteed to return a value which is unique
    * across all threads reading from this `supply`.
    */
  def supply[F[_]](initial: Long)(implicit async: Async[F]): Stream[F, Long] = {
    import java.util.concurrent.atomic.AtomicLong
    val l = new AtomicLong(initial)
    Stream.repeatEval { async.delay { l.getAndIncrement }}
  }


  def reorder[A, F[_]](startVal: Long = 0): Pipe[F, (A, Long), A] = {
    def go(nextVal: Long, v: Vector[(A, Long)]): Pipe[F, (A, Long), A] = {
      Stream.receive1[(A, Long), A] { l =>
        if (l._2 == nextVal) {
          val sorted: Vector[(A, Long)] = v.sortBy(_._2)
          val contiguous: Vector[(A, Long)] = sorted.foldLeft(Vector(l))((acc, i) => if (i._2 == acc.head._2 + 1) i +: acc else acc).reverse
          val all: Process1[(A, Long), A] = Process.emitAll(contiguous.map(_._1))
          all ++ go(contiguous.map(_._2).max + 1, sorted.filterNot(contiguous.contains))
        }
        else
          go(nextVal, l +: v)
      }
    }
    go(startVal, Vector.empty)
  }

  def orderedMergeN[A, B, F[_]](maxOpen: Int)(p: Stream[F, A])(f: A => Process[F, B])(implicit async:Async[F]): Stream[F, B] = {
    val tagged: Stream[F, (A, Long)] = p.zip(supply(0))
    val b: Stream[F, Stream[F, (B, Long)]] = tagged map (a => f(a._1).map(b => (b, a._2)))
    join(maxOpen)(b) through reorder(0)
  }
}
