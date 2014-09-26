#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from itertools import chain, ifilter, imap
import operator

from pyspark import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.util import rddToFileName, RDDFunction, RDDFunction2
from pyspark.rdd import portable_hash
from pyspark.streaming.duration import Duration, Seconds
from pyspark.resultiterable import ResultIterable

__all__ = ["DStream"]


class DStream(object):
    def __init__(self, jdstream, ssc, jrdd_deserializer):
        self._jdstream = jdstream
        self._ssc = ssc
        self.ctx = ssc._sc
        self._jrdd_deserializer = jrdd_deserializer
        self.is_cached = False
        self.is_checkpointed = False

    def context(self):
        """
        Return the StreamingContext associated with this DStream
        """
        return self._ssc

    def count(self):
        """
        Return a new DStream which contains the number of elements in this DStream.
        """
        return self.mapPartitions(lambda i: [sum(1 for _ in i)]).sum()

    def sum(self):
        """
        Add up the elements in this DStream.
        """
        return self.mapPartitions(lambda x: [sum(x)]).reduce(operator.add)

    def print_(self, label=None):
        """
        Since print is reserved name for python, we cannot define a "print" method function.
        This function prints serialized data in RDD in DStream because Scala and Java cannot
        deserialized pickled python object. Please use DStream.pyprint() to print results.

        Call DStream.print() and this function will print byte array in the DStream
        """
        # a hack to call print function in DStream
        getattr(self._jdstream, "print")(label)

    def filter(self, f):
        """
        Return a new DStream containing only the elements that satisfy predicate.
        """
        def func(iterator):
            return ifilter(f, iterator)
        return self.mapPartitions(func, True)

    def flatMap(self, f, preservesPartitioning=False):
        """
        Pass each value in the key-value pair DStream through flatMap function
        without changing the keys: this also retains the original RDD's partition.
        """
        def func(s, iterator):
            return chain.from_iterable(imap(f, iterator))
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def map(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each element of DStream.
        """
        def func(iterator):
            return imap(f, iterator)
        return self.mapPartitions(func, preservesPartitioning)

    def mapPartitions(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each partition of this DStream.
        """
        def func(s, iterator):
            return f(iterator)
        return self.mapPartitionsWithIndex(func, preservesPartitioning)

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """
        Return a new DStream by applying a function to each partition of this DStream,
        while tracking the index of the original partition.
        """
        return self.transform(lambda rdd: rdd.mapPartitionsWithIndex(f, preservesPartitioning))

    def reduce(self, func):
        """
        Return a new DStream by reduceing the elements of this RDD using the specified
        commutative and associative binary operator.
        """
        return self.map(lambda x: (None, x)).reduceByKey(func, 1).map(lambda x: x[1])

    def reduceByKey(self, func, numPartitions=None):
        """
        Merge the value for each key using an associative reduce function.

        This will also perform the merging locally on each mapper before
        sending results to reducer, similarly to a "combiner" in MapReduce.

        Output will be hash-partitioned with C{numPartitions} partitions, or
        the default parallelism level if C{numPartitions} is not specified.
        """
        return self.combineByKey(lambda x: x, func, func, numPartitions)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None):
        """
        Count the number of elements for each key, and return the result to the
        master as a dictionary
        """
        def func(rdd):
            return rdd.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions)
        return self.transform(func)

    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        """
        Return a copy of the DStream partitioned using the specified partitioner.
        """
        return self.transform(lambda rdd: rdd.partitionBy(numPartitions, partitionFunc))

    def foreach(self, func):
        return self.foreachRDD(lambda rdd, _: rdd.foreach(func))

    def foreachRDD(self, func):
        """
        Apply userdefined function to all RDD in a DStream.
        This python implementation could be expensive because it uses callback server
        in order to apply function to RDD in DStream.
        This is an output operator, so this DStream will be registered as an output
        stream and there materialized.
        """
        jfunc = RDDFunction(self.ctx, func, self._jrdd_deserializer)
        self.ctx._jvm.PythonForeachDStream(self._jdstream.dstream(), jfunc)

    def pyprint(self):
        """
        Print the first ten elements of each RDD generated in this DStream. This is an output
        operator, so this DStream will be registered as an output stream and there materialized.
        """
        def takeAndPrint(rdd, time):
            """
            Closure to take element from RDD and print first 10 elements.
            This closure is called by py4j callback server.
            """
            taken = rdd.take(11)
            print "-------------------------------------------"
            print "Time: %s" % (str(time))
            print "-------------------------------------------"
            for record in taken[:10]:
                print record
            if len(taken) > 10:
                print "..."
            print

        self.foreachRDD(takeAndPrint)

    def mapValues(self, f):
        """
        Pass each value in the key-value pair RDD through a map function
        without changing the keys; this also retains the original RDD's
        partitioning.
        """
        map_values_fn = lambda (k, v): (k, f(v))
        return self.map(map_values_fn, preservesPartitioning=True)

    def flatMapValues(self, f):
        """
        Pass each value in the key-value pair RDD through a flatMap function
        without changing the keys; this also retains the original RDD's
        partitioning.
        """
        flat_map_fn = lambda (k, v): ((k, x) for x in f(v))
        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def glom(self):
        """
        Return a new DStream in which RDD is generated by applying glom() to RDD of
        this DStream. Applying glom() to an RDD coalesces all elements within each partition into
        an list.
        """
        def func(iterator):
            yield list(iterator)
        return self.mapPartitions(func)

    def cache(self):
        """
        Persist this DStream with the default storage level (C{MEMORY_ONLY_SER}).
        """
        self.is_cached = True
        self.persist(StorageLevel.MEMORY_ONLY_SER)
        return self

    def persist(self, storageLevel):
        """
        Set this DStream's storage level to persist its values across operations
        after the first time it is computed. This can only be used to assign
        a new storage level if the DStream does not have a storage level set yet.
        """
        self.is_cached = True
        javaStorageLevel = self.ctx._getJavaStorageLevel(storageLevel)
        self._jdstream.persist(javaStorageLevel)
        return self

    def checkpoint(self, interval):
        """
        Mark this DStream for checkpointing. It will be saved to a file inside the
        checkpoint directory set with L{SparkContext.setCheckpointDir()}

        @param interval: Time interval after which generated RDD will be checkpointed
               interval has to be pyspark.streaming.duration.Duration
        """
        self.is_checkpointed = True
        self._jdstream.checkpoint(interval._jduration)
        return self

    def groupByKey(self, numPartitions=None):
        """
        Return a new DStream which contains group the values for each key in the
        DStream into a single sequence.
        Hash-partitions the resulting RDD with into numPartitions partitions in
        the DStream.

        Note: If you are grouping in order to perform an aggregation (such as a
        sum or average) over each key, using reduceByKey will provide much
        better performance.

        """
        return self.transform(lambda rdd: rdd.groupByKey(numPartitions))

    def countByValue(self):
        """
        Return new DStream which contains the count of each unique value in this
        DStreeam as a (value, count) pairs.
        """
        return self.map(lambda x: (x, None)).reduceByKey(lambda x, y: None).count()

    def saveAsTextFiles(self, prefix, suffix=None):
        """
        Save this DStream as a text file, using string representations of elements.
        """

        def saveAsTextFile(rdd, time):
            """
            Closure to save element in RDD in DStream as Pickled data in file.
            This closure is called by py4j callback server.
            """
            path = rddToFileName(prefix, suffix, time)
            rdd.saveAsTextFile(path)

        return self.foreachRDD(saveAsTextFile)

    def saveAsPickleFiles(self, prefix, suffix=None):
        """
        Save this DStream as a SequenceFile of serialized objects. The serializer
        used is L{pyspark.serializers.PickleSerializer}, default batch size
        is 10.
        """

        def saveAsPickleFile(rdd, time):
            """
            Closure to save element in RDD in the DStream as Pickled data in file.
            This closure is called by py4j callback server.
            """
            path = rddToFileName(prefix, suffix, time)
            rdd.saveAsPickleFile(path)

        return self.foreachRDD(saveAsPickleFile)

    def collect(self):
        result = []

        def get_output(rdd, time):
            r = rdd.collect()
            result.append(r)
        self.foreachRDD(get_output)
        return result

    def transform(self, func):
        return TransformedDStream(self, lambda a, t: func(a), True)

    def transformWithTime(self, func):
        return TransformedDStream(self, func, False)

    def transformWith(self, func, other, keepSerializer=False):
        jfunc = RDDFunction2(self.ctx, func, self._jrdd_deserializer)
        dstream = self.ctx._jvm.PythonTransformed2DStream(self._jdstream.dstream(),
                                                          other._jdstream.dstream(), jfunc)
        jrdd_serializer = self._jrdd_deserializer if keepSerializer else self.ctx.serializer
        return DStream(dstream.asJavaDStream(), self._ssc, jrdd_serializer)

    def repartitions(self, numPartitions):
        return self.transform(lambda rdd: rdd.repartition(numPartitions))

    def union(self, other):
        return self.transformWith(lambda a, b: a.union(b), other, True)

    def cogroup(self, other):
        return self.transformWith(lambda a, b: a.cogroup(b), other)

    def leftOuterJoin(self, other):
        return self.transformWith(lambda a, b: a.leftOuterJion(b), other)

    def rightOuterJoin(self, other):
        return self.transformWith(lambda a, b: a.rightOuterJoin(b), other)

    def _jtime(self, milliseconds):
        return self.ctx._jvm.Time(milliseconds)

    def slice(self, begin, end):
        jrdds = self._jdstream.slice(self._jtime(begin), self._jtime(end))
        return [RDD(jrdd, self.ctx, self._jrdd_deserializer) for jrdd in jrdds]

    def window(self, windowDuration, slideDuration=None):
        d = Seconds(windowDuration)
        if slideDuration is None:
            return DStream(self._jdstream.window(d), self._ssc, self._jrdd_deserializer)
        s = Seconds(slideDuration)
        return DStream(self._jdstream.window(d, s), self._ssc, self._jrdd_deserializer)

    def reduceByWindow(self, reduceFunc, invReduceFunc, windowDuration, slideDuration):
        keyed = self.map(lambda x: (1, x))
        reduced = keyed.reduceByKeyAndWindow(reduceFunc, invReduceFunc,
                                             windowDuration, slideDuration, 1)
        return reduced.map(lambda (k, v): v)

    def countByWindow(self, windowDuration, slideDuration):
        return self.map(lambda x: 1).reduceByWindow(operator.add, operator.sub,
                                                    windowDuration, slideDuration)

    def countByValueAndWindow(self, windowDuration, slideDuration, numPartitions=None):
        keyed = self.map(lambda x: (x, 1))
        counted = keyed.reduceByKeyAndWindow(lambda a, b: a + b, lambda a, b: a - b,
                                             windowDuration, slideDuration, numPartitions)
        return counted.filter(lambda (k, v): v > 0).count()

    def groupByKeyAndWindow(self, windowDuration, slideDuration, numPartitions=None):
        ls = self.mapValues(lambda x: [x])
        grouped = ls.reduceByKeyAndWindow(lambda a, b: a.extend(b) or a, lambda a, b: a[len(b):],
                                          windowDuration, slideDuration, numPartitions)
        return grouped.mapValues(ResultIterable)

    def reduceByKeyAndWindow(self, func, invFunc,
                             windowDuration, slideDuration, numPartitions=None):
        reduced = self.reduceByKey(func)

        def reduceFunc(a, t):
            return a.reduceByKey(func, numPartitions)

        def invReduceFunc(a, b, t):
            b = b.reduceByKey(func, numPartitions)
            joined = a.leftOuterJoin(b, numPartitions)
            return joined.mapValues(lambda (v1, v2): invFunc(v1, v2) if v2 is not None else v1)

        if not isinstance(windowDuration, Duration):
            windowDuration = Seconds(windowDuration)
        if not isinstance(slideDuration, Duration):
            slideDuration = Seconds(slideDuration)
        serializer = reduced._jrdd_deserializer
        jreduceFunc = RDDFunction(self.ctx, reduceFunc, reduced._jrdd_deserializer)
        jinvReduceFunc = RDDFunction2(self.ctx, invReduceFunc, reduced._jrdd_deserializer)
        dstream = self.ctx._jvm.PythonReducedWindowedDStream(reduced._jdstream.dstream(),
                                                             jreduceFunc, jinvReduceFunc,
                                                             windowDuration._jduration,
                                                             slideDuration._jduration)
        return DStream(dstream.asJavaDStream(), self._ssc, serializer)

    def updateStateByKey(self, updateFunc):
        # FIXME: convert updateFunc to java JFunction2
        jFunc = updateFunc
        return self._jdstream.updateStateByKey(jFunc)


class TransformedDStream(DStream):
    def __init__(self, prev, func, reuse=False):
        ssc = prev._ssc
        self._ssc = ssc
        self.ctx = ssc._sc
        self._jrdd_deserializer = self.ctx.serializer
        self.is_cached = False
        self.is_checkpointed = False

        if (isinstance(prev, TransformedDStream) and
                not prev.is_cached and not prev.is_checkpointed):
            prev_func = prev.func
            old_func = func
            func = lambda rdd, t: old_func(prev_func(rdd, t), t)
            reuse = reuse and prev.reuse
            prev = prev.prev

        self.prev = prev
        self.func = func
        self.reuse = reuse
        self._jdstream_val = None

    @property
    def _jdstream(self):
        if self._jdstream_val is not None:
            return self._jdstream_val

        jfunc = RDDFunction(self.ctx, self.func, self.prev._jrdd_deserializer)
        jdstream = self.ctx._jvm.PythonTransformedDStream(self.prev._jdstream.dstream(),
                                                          jfunc, self.reuse).asJavaDStream()
        self._jdstream_val = jdstream
        return jdstream
