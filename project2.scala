import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random
import scala.concurrent.duration._

object project2 {
	def main(args: Array[String]){
		val GOSSIP_MAX: Int = 10
		val PUSH_SUM_MAX: Int = 3
		val TRANSMIT_NUMBER: Int = 5
		val msg: String = "Gossip"

		case class CreateTopology()
		case class Done(myID: String)
		case class Gossip(msg: String)
		case class PushSum(s:Double, w:Double)

		println("project2 - Gossip Protocol")
		class Master(numNodes: Int, topology: String, algorithm: String) extends Actor{

			var converged: Boolean = false
			var startTime: Long = _

			def receive = {
				case CreateTopology() =>
					
					if(topology == "full"){
						println("Creating the full topology")
						for(i <- 0 until numNodes){
							var myID: String = i.toString
							val act = context.actorOf(Props(new Node(myID, numNodes, topology, algorithm)),name=myID)
						}
						startTime = System.currentTimeMillis
						var pivot: String = (Random.nextInt(numNodes)).toString
						println("Starting the algorithm from node = " + pivot)
						if(algorithm == "gossip"){
							context.actorSelection(pivot) ! Gossip(msg)
						} else if(algorithm == "push-sum") {
							context.actorSelection(pivot) ! PushSum(0,0)
						}
					} 
					if(topology == "3D"){

					} 
					if(topology == "line"){

					} 
					if(topology == "imp3D"){

					} 

				case "Done" =>
					var totalTime: Long = System.currentTimeMillis - startTime
					println("Time to converge = " + totalTime.millis)
					context.system.shutdown()

			}
		}

		class Node(myID: String, numNodes: Int, topology: String, algorithm: String) extends Actor{

			
			var count: Int = _
			var message: String = _
			var s: Double=_
			var w : Double=_
			var pcount : Int=_

			def getNext(): String = {
				topology match {
					case "full" =>
						(Random.nextInt(numNodes)).toString
					// case "3D" =>
					//case "line" =>

					// case "imp3D" =>
				}
			}

			def receive = {
				case Gossip(msg: String) =>
					// println("Got msg for node = " + myID)
					// Randomly select a neighbor and send the gossip
					if(count <= GOSSIP_MAX-1){
						this.message = msg
						count += 1
						var next: String = getNext()
						while(next == myID) next = getNext()
						// println("Sent msg to node = " + next)
						context.actorSelection("../"+next) ! Gossip(msg)
					} else if(count == GOSSIP_MAX) {
						context.parent ! "Done"
					} 
				case PushSum(s: Double, w: Double) =>

					if(Math.abs((this.s/this.w)- ((this.s+s)/(this.w+w))) <=1E-10){
						pcount += 1
					} else {
						pcount = 0
					}
					this.s = this.s + s
					this.w = this.w + w
					this.s = this.s/2
					this.w = this.w/2
					
					if(pcount == PUSH_SUM_MAX-1){
						context.parent ! "Done"
					}
					var next : String = getNext()
					while(next == myID) next = getNext()
					context.actorSelection("../"+next) ! PushSum(this.s,this.w)
			}
		}

		if(args.size<3){
			println("Enter valid number of inputs!!")
		} else {
			val system = ActorSystem("MasterSystem")
			val master = system.actorOf(Props(new Master(args(0).toInt, args(1), args(2))), name = "master")
			master ! CreateTopology()
		}

	}	
}