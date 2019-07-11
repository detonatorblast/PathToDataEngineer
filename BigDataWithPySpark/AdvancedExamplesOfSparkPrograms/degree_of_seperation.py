from pyspark import SparkContext, SparkConf

sconf = SparkConf().setMaster("local").setAppName("DegreeOfSeperation")

sc = SparkContext(conf=sconf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)

def convertToBFS(line):
    distance = 9999
    color = 'WHITE'
    connections = []

    heroes = line.split()

    startHeroId = int(heroes[0])
    for connHeroId in heroes[1:]:
        connections.append(int(connHeroId))

    if startHeroId == startCharacterID:
        distance = 0
        color = 'GREY'

    return (startHeroId, (connections, distance, color))


def bfsMap(node):
    characterId = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    result = []

    if color == "GREY":
        for connection in connections:
            newDistance = distance + 1
            newCharacterId = connection
            newColor = "GREY"
            if newCharacterId == targetCharacterID:
                hitCounter.add(1)

            newEntry = (newCharacterId, ([], newDistance, newColor))
            result.append(newEntry)

        #We've processed this node, so color it black
        color = "BLACK"

    result.append((characterId, (connections, distance, color)))
    return result


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    
    distance1 = data1[1]
    distance2 = data2[1]
    
    color1 = data1[2]
    color2 = data2[2]
    
    distance = 9999
    color = color1
    edges = []
    
    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)
        
    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1
        
    if (distance2 < distance):
        distance = distance2
        
     # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GREY' or color2 == 'BLACK')):
        color = color2
    if (color1 == 'GREY' and color2 == 'BLACK'):
        color = color2 
    if (color2 == 'WHITE' and (color1 == 'GREY' or color1 == 'BLACK')):
        color = color1
    if (color2 == 'GREY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)      
         

def createStartingRdd():
    inputFile = sc.textFile("/home/muktesh/Datasets/Marvel-graph.txt")
    return inputFile.map(convertToBFS)

if __name__ == '__main__':
    heroConnectionGraph = createStartingRdd()

#    for node in heroConnectionGraph.collect():
#        if node[0] == startCharacterID:
#            print(node)
   
    for iteration in range(0, 10):
        print("Running BFS iteration# " + str(iteration+1))

        # Create new vertices as needed to darken or reduce distances in the
        # reduce stage. If we encounter the node we're looking for as a GRAY
        # node, increment our accumulator to signal that we're done.
        mapped = heroConnectionGraph.flatMap(bfsMap)

        # Note that mapped.count() action here forces the RDD to be evaluated, and
        # that's the only reason our accumulator is actually updated.
        print("Processing " + str(mapped.count()) + " values.")

        if (hitCounter.value > 0):
            print("Hit the target character! From " + str(hitCounter.value) \
                    + " different direction(s).")
            break

        # Reducer combines data for each character ID, preserving the darkest
        # color and shortest path.
        heroConnectionGraph = mapped.reduceByKey(bfsReduce)
        
