
import akka.actor._
import scala.concurrent.duration._

class Gossip(numNodes: Int, topology: String) extends Actor {
  
  var validNeighbours: Int = 0
  var rumorCount: Int = 0
  var neighbours = new scala.collection.mutable.ArrayBuffer[Int] 
  var dead: Boolean = false
  var root: Int = 0
  var myName: Int = self.path.name.toInt
  var allActiveNodes = new scala.collection.mutable.ArrayBuffer[Int] 
  var schedulerFlag: Cancellable = _
  val system = context.system
  
  override def preStart() = {
    
    for(i <- 0 until numNodes) {
        allActiveNodes += i 
    }
    
	topology match
	{
		case "2D" =>		arrange2DTopology()
	
		case "line" =>		arrangeLineTopology()
	
		case "imp2D"=>		arrangeimp2DTopology()
							  
							  
		case "full" =>		validNeighbours = numNodes - 1
							
							for(i <- 0 until numNodes) {
								
								if(myName != i) 
									neighbours += i
								
							}
							
	}
	
//	for(i <-  neighbours) {
//		println(self.path.name + " " + i)
//	}	
	
  }

  def receive = {

     case "rumor" =>	if(rumorCount == 0) {
       
					    	 import system.dispatcher
					    	 schedulerFlag = context.system.scheduler.schedule(0 seconds, 5 milliseconds, self, "sendMessage")
     					}
     
     					if(rumorCount < 3)
     						rumorCount = rumorCount + 1
     					else {
     					  if(!dead) {
     						  schedulerFlag.cancel;
     						  dead = true;
     						  context.actorSelection("../Master") ! false
     					  }
     					  else {
     					    sender ! "terminated"
     					  }
     					}
     					//println("Rumor count for " + self.path.name + " is " + rumorCount)
     						
     case "sendMessage" =>	var randomNode = getRandomNode()
     						
//     						 println("Sending rumor from " + self.path.name + " to " + neighbours(randomNode))
       						context.actorSelection("../" + neighbours(randomNode).toString) ! "rumor"
     						
     case "terminated" => 	//println("terminated received from " + sender.path.name + " to " + self.path.name)
     						
    		 				var index: Int = -1
    		 				if(neighbours.contains(sender.path.name.toInt)) {
    		 				  
    		 					index = neighbours.indexOf(sender.path.name.toInt)
    		 					neighbours.remove(index)
    		 					validNeighbours = validNeighbours - 1		
    		 				}
     
     						
//     						for(i <-  neighbours) {
//     							println(self.path.name + " " + i)
//     						}
//     						println()
     
     						if(allActiveNodes.contains(sender.path.name.toInt)) {
     							index = allActiveNodes.indexOf(sender.path.name.toInt)
     							allActiveNodes.remove(index)
     							
     							if(allActiveNodes.length == 1){
     								
     							}else{
     							
     							  if(topology.equals("line")) {
     								arrangeLineTopology()
     							}
     							
     							if(topology.equals("2D")) {
     								arrange2DTopology()
     							}
     							
     							if(topology.equals("imp2D")) {
     								arrangeimp2DTopology()
     							}

     							}
     								
     						}
     				
  }
  
  def getRandomNode(): Int = {
   
    var x:Int = -1
    if(validNeighbours != 0)
    	x = (math.floor(math.random * validNeighbours)).toInt 
    x
  }
  
  def getRandomConnection(up: Int, down: Int, left: Int, right: Int): Int = {
    
    var x:Int = -2
    var flag: Boolean = true
    var temp = new scala.collection.mutable.ArrayBuffer[Int]
    
    for(i <- 0 until numNodes) {
    	
    	if(i != up && i != down && i != left && i != right && i != myName)
    	  temp += i
    }
    
	
	if(!temp.isEmpty) {
	  
		var index = (math.floor(math.random * temp.length)).toInt
		x = temp(index)		
	}
	  
    x
  }
  
  def arrangeLineTopology() {
	  	var index: Int = allActiveNodes.indexOf(myName)
	  			
	  			if(!neighbours.isEmpty) {
	  				neighbours.clear
	  			}
	  			validNeighbours = 0
	  	
			if(index == 0) {
				neighbours += allActiveNodes(index + 1)
				validNeighbours = 1
			}else if(index == allActiveNodes.length - 1) {
				neighbours += allActiveNodes(allActiveNodes.length - 2)
				validNeighbours = 1
			}else {
				neighbours += allActiveNodes(index - 1)
				neighbours += allActiveNodes(index + 1)
				validNeighbours = 2
			}
  }
  
  def arrange2DTopology() {
    
    root =  math.ceil(Math.sqrt(allActiveNodes.length.toDouble)).toInt
							
	var up: Int = -1
	var down: Int = -1
	var left: Int = -1
	var right: Int = -1
							
	var index: Int = allActiveNodes.indexOf(myName)
	
	if(!neighbours.isEmpty) {
		neighbours.clear
	}
    
	if(index - root >= 0) {
								
		up = allActiveNodes(index - root)
		validNeighbours = validNeighbours + 1
	}
								
	if(index + root < allActiveNodes.length) {
								  
		down = allActiveNodes(index + root)
		validNeighbours = validNeighbours + 1
	}
								
	if(index % root != 0) {
								  
		left = allActiveNodes(index - 1)
		validNeighbours = validNeighbours + 1
	}
								
	if(index % root != root - 1 && index != allActiveNodes.length - 1) {
								  
		right = allActiveNodes(index + 1)
		validNeighbours = validNeighbours + 1
	}
		
	if(up != -1) 
		neighbours += up
							 
	if(down != -1) 
		neighbours += down
							
	if(left != -1) 
		neighbours += left
							
	if(right != -1) 
		neighbours += right
  }
  
  
  def arrangeimp2DTopology() {
    
   root =  math.ceil(Math.sqrt(allActiveNodes.length.toDouble)).toInt
							
	var up: Int = -1
	var down: Int = -1
	var left: Int = -1
	var right: Int = -1
						
	var index: Int = allActiveNodes.indexOf(myName)
	
	if(!neighbours.isEmpty) {
		neighbours.clear
	}
    
	if(index - root >= 0) {
								
		up = allActiveNodes(index - root)
		validNeighbours = validNeighbours + 1
	}
								
	if(index + root < allActiveNodes.length) {
								  
		down = allActiveNodes(index + root)
		validNeighbours = validNeighbours + 1
	}
								
	if(index % root != 0) {
								  
		left = allActiveNodes(index - 1)
		validNeighbours = validNeighbours + 1
	}
								
	if(index % root != root - 1 && index != allActiveNodes.length - 1) {
								  
		right = allActiveNodes(index + 1)
		validNeighbours = validNeighbours + 1
	}
		
	var random = getRandomConnection(up, down, left, right)
	
	if(up != -1) 
		neighbours += up
							 
	if(down != -1) 
		neighbours += down
							
	if(left != -1) 
		neighbours += left
							
	if(right != -1) 
		neighbours += right
		
	if(random != -2)
		neighbours += random
  }
  
}