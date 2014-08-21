import sys
from operator import add

from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: wordcount <hostname> <port>"
        exit(-1)
    ssc = StreamingContext(appName="PythonStreamingNetworkWordCount", 
                           duration=Seconds(1))

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    words = lines.flatMap(lambda line: line.split(" "))
    mapped_words = words.map(lambda word: (word, 1))
    count = mapped_words.reduceByKey(add)
    count.pyprint()

    ssc.start()
    ssc.awaitTermination()
