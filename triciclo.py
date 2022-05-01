
from pyspark import SparkContext, SparkConf
from pprint import pprint
import sys


def rdd_distinct(sc, filename):
    return sc.textFile(filename).\
        map(edges).\
        filter(lambda x: x is not None).\
        distinct()
        
def edges(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
        return (n1, n2)
    elif n1 > n2:
        return (n2, n1)
    else:
        pass
        
def process(tuple):
    key = tuple[0]
    adj = tuple[1]
    aux = []
    for i in range(0, len(adj)):
        node1=adj[i]
        aux.append(((key,node1),"e"))
        for j in range(i, len(adj)):
            node2=adj[j]
            if node1!=node2:
                if node1 < node2:
                    aux.append(((node1, node2),('p',key)))
                else:
                    aux.append(((node2, node1),('p',key)))
    return (key, aux)

def unir_adj(tuple):
    key = tuple[0]
    adj = tuple[1]
    for node in adj:
        if node != "e":
            key = key + (node[1],)
    return key

def cicle(sc, filename):
    nodes = rdd_distinct(sc, filename)
    adj = nodes.groupByKey().\
        mapValues(list).\
        map(process).\
        flatMap(lambda x: tuple(x[1])).\
        groupByKey().\
        mapValues(list).\
        filter(lambda x: len(x[1])>1).\
        mapValues(list).\
        map(unir_adj)
        
    return(adj)

def main(sc, files):
	rdd_list = []
	for file_name in files:
		file_rdd = cicle(sc, file_name)

		rdd_list.append(file_rdd)

	rdd_final = sc.union(rdd_list)
	pprint(rdd_final.collect())

    
if __name__ == "__main__":
    conf = SparkConf().setAppName("Ciclos")
    sc = SparkContext(conf=conf)

    if len(sys.argv) == 1:
        print("Error faltan argumentos")

    else:
        main(sc, sys.argv[1:])

    sc.stop()