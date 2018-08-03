from src.main.python.com.dt.learn.PySparkContextApplication import PySparkContext


class RddOperation(object):
    def rdd2Map(self):
        rdd = PySparkContext.createRddForSimple(PySparkContext.createSparkContext())
        rdd2Map = rdd.map(lambda x:x*x).collect()
        for num in rdd2Map:
            print(num)