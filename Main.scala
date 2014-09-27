
import akka.actor._

object MainProg {
  
  sealed trait MessageType
  case class ComputeSum(s: Double, w: Double) extends MessageType
  

 def main(args: Array[String]): Unit = {
	
   if(args.length < 3) {
     println("Missing arguments!")
     println("Enter number of nodes, topology (full, 2D, line or imp2D) and algorithm (gossip or push-sum).")
   }
   else {
     
     if((args(1).equals("full") || args(1).equals("2D") || args(1).equals("line") || args(1).equals("imp2D"))
         &&
         (args(2).equals("gossip") || args(2).equals("push-sum")))
     {
       val system = ActorSystem("system")
       val master = system.actorOf(Props(new Master(args(0).toInt, args(1), args(2))) , "Master")
       
       master ! "start"
     }
     
     else {
       println("Incorrect arguments!")
       println("Enter number of nodes, topology (full, 2D, line or imp2D) and algorithm (gossip or push-sum).")
     }
       
   }
 }
}


class Master(numNodes: Int, topology: String, algorithm: String) extends Actor {
  
  var worker: ActorRef = _
  var activeNodes: Int = numNodes
  var timeStart: Long = 0
  
  
  override def preStart() {
    
		algorithm match {
			case "gossip" =>	for(i <- 0 until numNodes) {
									worker = context.system.actorOf(Props(new Gossip(numNodes, topology)), ""+i)
									//println(worker.path.name)
								}
								
		    case "push-sum" =>	for(i <- 0 until numNodes) {
									worker = context.system.actorOf(Props(new PushSum(numNodes, topology)), ""+i)
									//println(worker.path.name)
								}
		}
		
		timeStart = System.currentTimeMillis
    
  }
  
  def receive = {
    case "start" => 	algorithm match {
      						case "gossip" => 	worker ! "rumor"
      						case "push-sum" => 	worker ! MainProg.ComputeSum(0.0, 0.0)
    					}
    
    case false =>		if(activeNodes > 1) 
      						activeNodes = activeNodes - 1
    
      					//println("active nodes = " + activeNodes)	
    					if(activeNodes == 1) 
    					  	  context.system.shutdown
      
  }
  
  override def postStop() {
    var timeEnd = System.currentTimeMillis
    
    println("Time taken for " + numNodes + " actors to run " + algorithm + " for " + topology + " topology is " + (timeEnd - timeStart) + " msecs")
  }
}