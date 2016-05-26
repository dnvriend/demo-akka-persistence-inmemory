/*
 * Copyright 2016 Dennis Vriend
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dnvriend

import java.util.concurrent.TimeUnit

import akka.actor.{ Actor, ActorSystem, PoisonPill, Props }
import akka.event.LoggingReceive
import akka.persistence.PersistentActor
import akka.persistence.inmemory.query.journal.scaladsl.InMemoryReadJournal
import akka.persistence.query.{ EventEnvelope, PersistenceQuery }
import akka.stream.{ ActorMaterializer, Materializer }

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

object Counter {

  sealed trait Command

  final case class Increment(value: Int) extends Command

  final case class Decrement(value: Int) extends Command

  sealed trait Event

  final case class Incremented(value: Int) extends Event

  final case class Decremented(value: Int) extends Event

  case class CounterState(value: Int = 0) {
    def update(event: Event): CounterState = event match {
      case Incremented(incrementBy) ⇒ copy(value + incrementBy)
      case Decremented(decrementBy) ⇒ copy(value - decrementBy)
    }
  }

}

class Counter(override val persistenceId: String, id: Int)(implicit ec: ExecutionContext) extends PersistentActor {

  import Counter._

  private var state = CounterState()

  import scala.concurrent.duration._

  context.system.scheduler.schedule(0.seconds, 1.second, self, Increment(1))
  context.system.scheduler.schedule(0.seconds, 5.seconds, self, Decrement(1))
  context.system.scheduler.scheduleOnce(FiniteDuration(id * 10, TimeUnit.SECONDS), self, PoisonPill)

  private def handleEvent(event: Event): Unit = {
    state = state.update(event)
    println("==> Current state: " + state)
  }

  override def receiveRecover: Receive = {
    case event: Incremented ⇒ handleEvent(event)
    case event: Decremented ⇒ handleEvent(event)
  }

  override def receiveCommand: Receive = {
    case Increment(value) ⇒
      println(s"==> Incrementing with: $value")
      persist(Incremented(value))(handleEvent)

    case Decrement(value) ⇒
      println(s"==> Decrementing with: $value")
      persist(Decremented(value))(handleEvent)
  }

  override def aroundPostStop(): Unit = {
    println("====> Stopped: id" + id)
    super.aroundPostStop()
  }
}

object CounterReader {
  final case class Offset(x: Long = 0)
}
class CounterReader(readJournal: InMemoryReadJournal)(implicit ec: ExecutionContext, mat: Materializer) extends Actor {
  def schedulePoll(from: Long): Unit = {
    println("Scheduling from: " + from)
    context.system.scheduler.scheduleOnce(1.second, self, CounterReader.Offset(from))
  }

  schedulePoll(0)

  override def receive: Receive = LoggingReceive {
    case CounterReader.Offset(from) ⇒
      readJournal.currentEventsByPersistenceId("COUNTER", from, Long.MaxValue)
        .runForeach(println)
        .map(_ ⇒ schedulePoll(from + 1))
  }
}

object Launch extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = ActorMaterializer()
  lazy val readJournal: InMemoryReadJournal = PersistenceQuery(system).readJournalFor[InMemoryReadJournal](InMemoryReadJournal.Identifier)
  system.actorOf(Props(new Counter("COUNTER", 1)), "Counter")
  system.actorOf(Props(new CounterReader(readJournal)), "CounterReader")
}
