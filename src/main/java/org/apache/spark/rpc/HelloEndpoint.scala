package org.apache.spark.rpc


class HelloEndpoint(override val rpcEnv : RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi (msg) => {
      println(s"receive $msg")
      context.reply(s"hi, $msg")
    }

    case SayBye (msg) => {
      println(s"receive $msg")
      context.reply(s"bye, $msg")
    }

  }
}
case class SayBye(msg: Any)
case class SayHi(msg: Any)