#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3 as db
import datetime

today = (datetime.date.today()).strftime('%Y%m%d')
tfour = (datetime.date.today() + datetime.timedelta(4)).strftime('%Y%m%d')

conn = db.connect('/home/nagabcube/Works/OVF/ftpimport.sqlite')
c = conn.cursor()

c.execute("SELECT DISTINCT cellid FROM tp_zala ORDER BY 1")
cells = c.fetchall()

r = []
for i in range(len(cells)):
    for t in c.execute("SELECT * FROM tp_zala WHERE date >= '"+today+"' AND cellid = "+str(cells[i][0])):
        r.append(t)
c.close()
conn.close()

for i in range(len(r)):
    if r[i][2] == '03' and r[i][1] < today:
        print '%s %s %s %8.3f' % (r[i][0], r[i][1], r[i][2], r[i][3])
    else:
        precip = r[i][3]-r[i-1][3]
        if  precip < 0:
            precip = 0.000
        if r[i][1] >= tfour and r[i][2] in ('06','12','18'):
            print '%s %s %02d %8.3f' % (r[i][0], r[i][1], int(r[i][2]) - 3, precip / 2)             
        print '%s %s %s %8.3f' % (r[i][0], r[i][1], r[i][2], precip)
        if r[i][1] >= tfour and r[i][2] == '18':
            precip = r[i+1][3]-r[i][3]
            if  precip < 0:
                precip = 0.000            
            print '%s %s %02d %8.3f' % (r[i][0], r[i][1], int(r[i][2]) + 3, precip / 2)

