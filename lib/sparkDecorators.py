import csv

def collectResult(func):
	""" decorator to collect() an RDD """
	def SparkJob(*args):
		return func(*args).collect()
	return SparkJob

def cacheResult(func):
	""" decorator to collect() an RDD """
	def SparkJob(*args):
		return func(*args).cache()
	return SparkJob

def parseVector(lin, delimiter=","):
	#return np.array([i for i in csv.reader([lin])][0])
	return tuple([i for i in csv.reader([lin])][0])


class WriteRDD(object):
	""" decorator class with functions to
		write to a specific path """
	def __init__(self, path):

		self.path = path 

	def __call__(self, func):

		#func_nm = func.__name__
		#print '\n\t'+'write_start_time: '+datetime.now.__str__()

		def func_call(*args):
			return func(*args).saveAsTextFile(self.path)

		return func_call