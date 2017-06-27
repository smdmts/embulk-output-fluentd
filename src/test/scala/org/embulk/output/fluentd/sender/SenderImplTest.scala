package org.embulk.output.fluentd.sender

import akka.stream.scaladsl._
import akka.stream.scaladsl.Tcp._
import org.scalatest.FlatSpec

import org.embulk.output.fluentd.TestActorManager
import scala.concurrent.Future

class SenderImplTest extends FlatSpec {

  val dummyServer: Source[IncomingConnection, Future[ServerBinding]] =
    TestActorManager.dummyServer





}
