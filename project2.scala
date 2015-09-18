import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random
import scala.concurrent.duration._

object project2 {
	def main(args: Array[String]){
		val GOSSIP_MAX: Int = 10
		val msg: String = "Gossip"

		case class CreateTopology()
		case class Done(myID: String)
		case class Gossip(msg: String)

		println("project2 - Gossip Protocol")
		class Master(numNodes: Int, topology: String, algorithm: String) extends Actor{

			var converged: Boolean = false
			var count: Int = _
			var startTime: Long = _

			def receive = {
				case CreateTopology() =>
					
					if(topology == "full"){
						println("Creating the full topology")
						for(i <- 0 until numNodes){
							val myID: String = i.toString
							val act = context.actorOf(Props(new Node(myID, numNodes, topology, algorithm)),name=myID)
						}
						startTime = System.currentTimeMillis
						var pivot:String = (Random.nextInt(numNodes)).toString
						if(algorithm == "gossip"){
							context.actorSelection(pivot) ! Gossip(msg)
						} else {

						}
					} 
					if(topology == "3D"){

					} 
					if(topology == "line"){

					} 
					if(topology == "imp3D"){

					} 

				case "Done" =>
					count = count+1
					if(count == numNodes) {
						var totalTime: Long = System.currentTimeMillis - startTime
						println("Time to converge = " + totalTime.millis)
						context.system.shutdown()
					}

			}
		}

		class Node(myID: String, numNodes: Int, topology: String, algorithm: String) extends Actor{

			
			var count: Int = _
			var message: String = _

			def PushSum() = {
				// Randomly select a neighbor and send half of the sum and weight
			}

			def getNext(): String = {
				topology match {
					case "full" =>
						(Random.nextInt(numNodes)).toString
					// case "3D" =>
					// case "line" =>
					// case "imp3D" =>
				}
			}

			def receive = {
				case Gossip(msg: String) =>
					// Randomly select a neighbor and send the gossip
					if(count <= GOSSIP_MAX-1){
						this.message = msg
						count += 1
						var next: String = getNext()
						while(next != myID) next = getNext()
						context.actorSelection("../"+next) ! Gossip(msg)
					} else {
						var next: String = getNext()
						while(next != myID) next = getNext()
						context.actorSelection("../"+next) ! Gossip(msg)
						context.parent ! "Done"
					}
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