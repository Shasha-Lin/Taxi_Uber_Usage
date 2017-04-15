import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#WARNING: REQUIRES 'avg_tra_day.out', output of 'by_minutes.py'

d = pd.read_csv('avg_tra_day.out', names=['time', 'wek_count', 'wend_count'])
d['time'] = d['time'].str[2:-1]
d['wend_count'] = d['wend_count'].str[0:-1]

def to_min(string):
    h = string.split(':')[0]
    m = string.split(':')[1]
    return 60*int(h) + int(m)
d['time'] = d['time'].apply(to_min)
d[['wend_count', 'wek_count','time']] = d[['wend_count', 'wek_count','time']].apply(pd.to_numeric)

X = np.array(d.time)
Y2 = np.array(d.wend_count)#/208
Y = np.array(d.wek_count)#/522

plt.figure(figsize=(20,10))
plt.plot(X, Y, label='weekday pickups')
plt.plot(X, Y2, label='weekend pickups')
plt.xlabel('mins into day')
plt.ylabel('# of average pickups')
plt.legend()
plt.title('total pickups over day, 2015-2016')
plt.savefig('total_pickups.png')

X = np.array(d.time)
Y2 = np.array(d.wend_count)/208
Y = np.array(d.wek_count)/522

plt.figure(figsize=(20,10))
plt.plot(X, Y, label='weekday pickups')
plt.plot(X, Y2, label='weekend pickups')
plt.xlabel('mins into day')
plt.ylabel('# of average pickups')
plt.legend()
plt.title('avg pickups over day, 2015-2016')
plt.savefig('avg_picks_per_day.png')