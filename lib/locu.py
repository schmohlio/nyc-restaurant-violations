import sys
import os
import time
import random

import json
import csv

import requests as r

# SET AN ENVIRONMENT VARIABLE CALLED `LOCU_API_KEY`

class LocuApi(object):

	api_key = None

	def __init__(self, tag=''):

		self.tag = tag
		self._set_credentials()

	@classmethod
	def _set_credentials(cls):

		error = None
		#new_key = os.environ["LOCU_API_KEY"]
		new_key='d4308ad22909b15626c3c595c5ff24eff73b8aa8'

		if cls.api_key=='' or cls.api_key!=new_key:
			cls.api_key = new_key

		if cls.api_key is None or cls.api_key=='':
			e = Exception("no api key environment variable named LOCU_API_KEY")
			raise e

		return 

	def search_venue(self, name, tries=0):
		""" retrieve venue search json """
		url = 'http://api.locu.com/v1_0/venue/search/?'
		params = {	'api_key':self.__class__.api_key, 
					'name':name}
		result = r.get(url, params=params)

		if result.status_code in (500,503):
			if tries==5:
				e = Exception("too many queries; wait until tmmrw. exiting program")
				raise e
			else:
				time.sleep((2**tries) + random.random())
				self.search_venue(name, tries+1)

		return json.loads(result.text) if result.status_code==200 else None


def verify_venue(response, phone):

	def _parse_phone(p):
		return ''.join(e for e in p if e.isalnum()) if p else None

	if response is None: return None
	if response['objects'] is None: return None
	if len(response['objects'])==0: return None

	for i in response['objects']:
		if _parse_phone(i['phone'])==phone:
			return i

	return None


