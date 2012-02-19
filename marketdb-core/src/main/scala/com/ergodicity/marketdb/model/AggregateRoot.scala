package com.ergodicity.marketdb.model

trait Reaction[+T]

case class Accepted[+T](events: List[Any], result: T) extends Reaction[T]

case class Rejected(message: String) extends Reaction[Nothing]

object Behaviors {
  def behavior[T](callback: List[Any] => Reaction[T]) = new Behavior[T] {
    protected def apply(events: List[Any]) = callback(events)
  }

  def accept[T](result: T) = behavior(events => Accepted(events, result))

  def reject(message: String) = behavior(_ => Rejected(message))

  def record(event: Any) = behavior(events => Accepted(event :: events, ()))

  def guard(condition: Boolean, message: => String) =
    if (condition) accept() else reject(message)
}

import Behaviors._

trait Behavior[+A] {
  protected def apply(events: List[Any]): Reaction[A]

  def map[B](f: A => B) = flatMap(a => accept(f(a)))

  def flatMap[B](next: A => Behavior[B]) = behavior {events =>
    this(events) match {
      case Accepted(updatedEvents, result) => next(result)(updatedEvents)
      case Rejected(message) => Rejected(message)
    }
  }

  def reaction = apply(Nil)

  def changes = reaction.asInstanceOf[Accepted[_]].events

  def rejected = reaction.asInstanceOf[Rejected].message
}

protected class EventHandler[Event, +Result](callback: Event => Result) {
  def apply(event: Event) = record(event) flatMap (_ => accept(callback(event)))

  def applyFromHistory(event: Event) = callback(event)
}

trait EventSourced[+Event <: AnyRef] {
  def applyEvent: PartialFunction[AnyRef, EventSourced[Event]]

  protected def handler[A, B](callback: A => B) = new EventHandler(callback)

  protected def unhandled = handler {event: AnyRef =>
    sys.error("unhandled event " + event + " for " + this)
  }

  implicit protected def handlerToPartialFunction[A, B](handler: EventHandler[A, B])(implicit m: Manifest[A]) =
    new PartialFunction[AnyRef, B] {
      def apply(event: AnyRef) =
        if (isDefinedAt(event)) handler.applyFromHistory(event.asInstanceOf[A])
        else unhandled.applyFromHistory(event)

      def isDefinedAt(event: AnyRef) = m.erasure.isInstance(event)
    }
}

trait AggregateRoot[+Event <: AnyRef] extends EventSourced[Event]

trait AggregateFactory[AR <: AggregateRoot[Event], Event <: AnyRef] extends EventSourced[Event] {
  def loadFromHistory[T <: AR](history: Iterable[Event]): T = {
    val aggregate = applyEvent(history.head)
    (aggregate /: history.tail)(_.applyEvent(_)).asInstanceOf[T]
  }
}