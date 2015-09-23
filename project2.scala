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
		val msg: String = "Gossip"

		case class CreateTopology()
		case class Gossip(msg: String)
		case class PushSum(s:Double, w:Double)
		case class PushSumDone(sum: Double)

		println("project2 - Gossip Protocol")
		class Master(numNodes: Int, topology: String, algorithm: String) extends Actor{

			var converged: Boolean = false
			var startTime: Long = _

			def receive = {
				case CreateTopology() =>
					
					if(topology == "full" || topology == "line"){
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
							context.actorSelection(pivot) ! PushSum(0,1)
						}
					} 
					if(topology == "3D"){
						var size: Int = Math.cbrt(numNodes).toInt
						var count: Int = 0
						for(i <- 0 until size){
							for(j <- 0 until size){
								for(k <- 0 until size){
									if(count <= numNodes){
										var myID: String = ((((i.toString).concat(".")).concat(j.toString)).concat(".")).concat(k.toString)
										val act = context.actorOf(Props(new Node(myID, numNodes, topology, algorithm)),name=myID)
										count += 1
									}
								}
							}
						}
						startTime = System.currentTimeMillis
						var pivot: String = "0.0.0"
						println("Starting the algorithm from node = " + pivot)
						if(algorithm == "gossip"){
							context.actorSelection(pivot) ! Gossip(msg)
						} else if(algorithm == "push-sum") {
							context.actorSelection(pivot) ! PushSum(0,1)
						}
					} 
					
					if(topology == "imp3D"){

					} 

				case "Done" =>
					var totalTime: Long = System.currentTimeMillis - startTime
					println("****** Time to converge = " + totalTime.millis + " ******")
					context.system.shutdown()

				case PushSumDone(sum: Double) =>
					var totalTime: Long = System.currentTimeMillis - startTime
					println("****** Time to converge = " + totalTime.millis + " ******")
					println("****** Sum = " + sum + " ******")
					context.system.shutdown()

			}
		}

		class Node(myID: String, numNodes: Int, topology: String, algorithm: String) extends Actor{

			
			var count: Int = _
			var message: String = _
			var s: Double = myID.toDouble
			var w : Double = 0.0
			var pcount : Int = _
			var next: String = _ 

			def getNext(): String = {
				
				topology match {
					case "full" =>
						return (Random.nextInt(numNodes)).toString
					case "line" =>
						var x: Int = myID.toInt
						if(x == 0){
							return (x+1).toString
						} else if(x == numNodes-1) {
							return (x-1).toString
						} else {
							var ran: Int = Random.nextInt(2)
							if(ran == 0) return (x-1).toString
							else return (x+1).toString
						}
					// case "3D" =>
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
					if(pcount == PUSH_SUM_MAX){
						context.parent ! PushSumDone(this.s/this.w)
					} else {
						this.s = this.s + s
						this.w = this.w + w
						this.s = this.s/2
						this.w = this.w/2
						var next : String = getNext()
						while(next == myID) next = getNext()
						context.actorSelection("../"+next) ! PushSum(this.s,this.w)
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