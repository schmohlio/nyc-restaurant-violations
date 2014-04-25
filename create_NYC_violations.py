#!/usr/bin/env python 

# make sure to append /lib to PYTHONPATH, or uncomment:

# import sys
# sys.path.append('/home/matthew/nyc-restaurant-violations/lib/')

from sparkDecorators import *
from pyspark import SparkContext, SparkConf

import operator as op
from time import sleep
from datetime import datetime as dt 

DIR = '/home/matthew/nyc-restaurant-violations/' #PUT YOUR DIRECTORY HERE

CONF = SparkConf()\
		.setMaster("local")\
		.setAppName("violation staging")\
		.set("spark.executor.memory", "3g")

S3PATH = "s3n://nyc-restaurant-violations/"


class WebRDD(object):

	_COLS = {"CAMIS":0,"DBA":1,"BORO":2,"BUILDING":3,"STREET":4,
			 "ZIPCODE":5,"PHONE":6,"CUISINECODE":7,"INSPDATE":8,
			 "ACTION":9, "VIOLCODE":10,"SCORE":11,"CURRENTGRADE":12,
			 "GRADEDATE":13,"RECORDDATE":14
			}

	def __init__(self, con, file):

		self.con = con
		self.data = con.textFile(file)\
                    .map(lambda x: x.encode("ascii","ignore"))\
					.map(lambda x: parseVector(x))

		self.data_out = None

	@classmethod
	def colNum(cls, name):
		return cls._COLS[name]

	#@WriteRDD(S3PATH+'staged')
	@cacheResult
	def getViolationsYr(self):   
		""" calculate following on restaurant group-by level:
				- number of violations
				- number of inspections (unique dates)
				- safety grade (use max for dedup)
				- phone number (use max for dedup)"""

		#restaurant = self.colNum('CAMIS')
		biz = self.colNum('DBA')
		inspect_date = self.colNum('INSPDATE')
		phone = self.colNum('PHONE')
		grade = self.colNum('CURRENTGRADE')

		df = "%Y-%m-%d %H:%M:%S"

		return self.data\
                .filter(lambda x: x[phone]!='')\
				.map(
					lambda x: ((x[biz],x[phone],x[inspect_date]),
							   (int(dt.strptime(x[inspect_date],df).year==2012),
								int(dt.strptime(x[inspect_date],df).year==2013),
						 		 x[grade])))\
				.reduceByKey(
					lambda (x0,x1,x2),(y0,y1,y2): 
						(x0+y0,x1+y1,x2 if x2>y2 else y2))\
				.map(
					lambda ((x0,x1,y),(i,j,k)): ((x0,x1),
											(int(dt.strptime(y,df).year==2012),
								 			 int(dt.strptime(y,df).year==2013),
											 i,j,k)))\
				.reduceByKey(
					lambda (x0,x1,x2,x3,x4),(y0,y1,y2,y3,y4):
							(x0+y0,x1+y1,x2+y2,x3+y3,
							 x4 if x4>y4 else y4))\
				.map(lambda ((x0,x1),(y1,y2,y3,y4,y5)): (x0,x1,y1,y2,y3,y4,y5)) # unpack


if __name__=="__main__":

	sc = SparkContext(conf = CONF)

	# GET ALL RESTAURANTS WITH AT LEAST 
	# 1 INSPECTION in 2012
	res = WebRDD(sc, S3PATH+"WebExtract.txt")\
			.getViolationsYr()\
			.filter(lambda x: x[1]>0)\
			.collect()

	# WRITE TO FILE
	res = [{'biz':i[0],
            'phone_num':i[1],
			'inspections_in_2012':i[2],
			'inspections_in_2013':i[3],
			'violations_in_2012':i[4],
			'violations_in_2013':i[5],
			'grade':i[6]}
	   		for i in res]


	keys = res[0].keys()
	f = open(DIR+'output/NYC_violations.csv', 'wb')
	dict_writer = csv.DictWriter(f, keys)
	dict_writer.writer.writerow(keys)
	dict_writer.writerows(res)
