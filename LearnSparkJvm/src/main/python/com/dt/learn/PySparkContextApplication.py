from pyspark import SparkConf,SparkContext
class PySparkContext(object):
    def createSparkContext(self):
        conf = SparkConf().setMaster("local").setAppName("my app")
        sc = SparkContext(conf=conf)
        return sc
    def stopSparkContext(self,sc):
        sc.stop()
    def wordCount(self,sc):
        rdd = sc.textFile('/tmp/sparkStudy/thatGirl.txt')
        wordCount = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        # 缓存到本地
        wordCount.persist()
        # 取出第一个数据
        wordCount.first()
        # 保存到一个文件中
        wordCount.saveAsTextFile('/tmp/sparkStudy/pyword')

    #创建一个简单的 rdd
    def createRddForSimple(self,sc):
        # 直接使用集合创建一个简单的 rdd,这种创建方式是直接把所有的数据加载到内存中
        rdd = sc.parallelize([1,2,3,4,5,6,7])
        return rdd

