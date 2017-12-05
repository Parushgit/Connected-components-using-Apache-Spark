''' YOUR_FIRST_NAME Parush
 *  YOUR_LAST_NAME  Garg
 *  YOUR_UBIT_NAME  parushga
 '''
from pyspark import SparkContext, SparkConf
import sys

#To get the initial data from the file which contains graph edges
def getData(data):
    d = data.split(" ")
    t = [int(x) for x in d]
    yield (t[0], t[1])
    yield (t[1], t[0])

#To map (u,v), (v,u) after first large star operation. This is done as second time reading from graph is not required.
def MapForLargeStar(data):
    u = data[0]
    v = data[1]
    yield (u, v)
    yield (v, u)

#To do map for smallStar
def MapForSmallStar(data):
    u = data[0]
    v = data[1]
    #print u, v
    if v <= u:
        yield u, v
    else:
        yield v, u

#Large star operation
def largeStar(data):
    u, gamma = data
    miningamma = min(gamma)
    m = min(miningamma, u)
    for y in gamma:
        if y >= u:
            yield y, m

#Small star operation
def smallStar(data):
    u, gamma = data
    gamma = list(gamma)
    gamma.insert(len(gamma), u)
    m = min(gamma)
    for y in gamma:
        yield y, m

#Main function
'''
Algorithm is
Repeat
Large Star
Small Star
Until convergence
'''
if __name__ == "__main__":

    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf = conf)
    firstTime = True                                #First time true to read data from the file
    
    while True:
        if firstTime:
            #RDD created after applying large star operation
            rddAfterLargeStar = sc.textFile(sys.argv[1]).flatMap(getData).groupByKey().flatMap(largeStar).distinct()
        else:
            rddAfterLargeStar = rddAferSmallStar.flatMap(MapForLargeStar).groupByKey().flatMap(largeStar).distinct()
        firstTime = False

        #RDD created after applying small star operation
        rddAferSmallStar = rddAfterLargeStar.flatMap(MapForSmallStar).groupByKey().flatMap(smallStar).distinct()

        #diff value is 0 when there are no changes in both large star and small star
        diff = rddAfterLargeStar.subtract(rddAferSmallStar).union(rddAferSmallStar.subtract(rddAfterLargeStar)).count()
        if diff == 0:
            #print "No change now in Large Star and Small Star"
            #Source: https://stackoverflow.com/questions/39430416/how-to-remove-brackets-from-rdd-output
            rddAferSmallStar.map(lambda (k, v): "{0} {1}".format(k, v)).saveAsTextFile("output")
            break
    sc.stop()

'''
Apache spark writes the output to the multiple files. I am not making it to write in the single output file since 
having a single output file is only a good idea if you have very little data.
Output format - vertex label.
'''