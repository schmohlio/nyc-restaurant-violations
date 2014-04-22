####

# Analyze all restaurants with at least
# 1 inspection in 2012 (active since 2012)

####


import locu as loc

import pandas as pd
import numpy as np

import statsmodels.api as sm
import matplotlib.pyplot as plt
from statsmodels.formula.api import ols
from statsmodels.graphics.api import interaction_plot, abline_plot
from statsmodels.stats.anova import anova_lm


if __name__="__main__":


	try:
		dat = pd.read_csv('NYC_violations.csv')
	except:
		e = Exception(""" please run create_NYC_violations.py 
						and drop file in home dir""")
		raise e 


	# HOW DO GRADES INTERACT WITH VIOLATIONS IN 2013?

	dat['violations_in_2013'].hist(by=dat['grade'], histtype='stepfilled', alpha=.85, bins=30,
	                                                        color="#A60628", normed=True)

	stats = dat.groupby(['grade'])['violations_in_2013'].apply(lambda x: x.describe())

	stats.swaplevel(0,1).unstack(level=1).reset_index().to_html()

	fig, ax = plt.subplots(figsize=(8,6))
	fig = dat.boxplot('violations_per_inspection', 'grade', ax=ax, grid=False)

	viols_lm = ols('violations_per_inspection ~ C(grade)', data=dat).fit()
	table1 = anova_lm(viols_lm)
	print table1
	print viols_lm.summary()

	# WHERE ARE THE RESTAURANTS WITH THE 
	# MOST VIOLATIONS PER INSPECTION LOCATED?

	worst = dat.sort(['violations_per_inspection']).tail(500)

	worst = [dict(j) for i,j in worst.iterrows()]

	t = loc.LocuApi('grab has_menu attr from list')

	worst_matches = []
	for j in worst:
	    tmp = loc.verify_venue(t.search_venue(j['biz']),j['phone_num'])
	    if tmp:
	        worst_matches.append(dict(j.items()+tmp.items()))

	pd.DataFrame(worst_matches)[['biz','lat','long','has_menu']].to_csv('/output/top_violations.csv', index=False)

