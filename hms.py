#!/usr/bin/env python
# -*- coding: utf-8 -*-
###############################################################################
# $Id$
#
# Project:  Balaton, Zala HMS: meteorological data conversation utility
# Purpose:  Loading specified datasources from FTP to SQLite and Hec-DSS
# Author:   Gabor, Nagy <nagabcube@gmail.com>
# 
###############################################################################
# Copyright (c) 2015, Gabor, Nagy <nagabcube@gmail.com>
# 
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
###############################################################################
import sys, os, datetime, string
import gdal
import sqlite3 as sqldb 

from ftplib import FTP

from vtools.data.api import *
from vtools.datastore.dss.api import *
from vtools.functions.api import *

# -- globalis kornyezeti valtozok, ezek modosithatok:

PTH_WORK = 'D:/work/'
DSS_BTON = 'Balaton-2013.dss'
DSS_ZALA = 'zala.dss'

###

def getFileList(remote_dir):

    entries  = []

    try:
        f = FTP('ftp.ovf.hu', 'ftp-user', 'ftp-pwd')
        f.cwd(remote_dir)
        f.dir(entries.append)
    except Exception, msg :
        print 'HIBA: Nem tudok kapcsolodni az FTP sitehoz, a program leallt.'
        f.quit()
        sys.exit(-1)

    return (f, entries)

###

def dwl_hqc(station):
	f, hqc_files = getFileList('hqcsap')	

	for entry in hqc_files:
		fname = entry.split()[3][12:-4]
		if fname == station and entry.split()[3].startswith('HmCsapnyers_'):
			lf = open('%s.hqc' % fname, 'wb')
			f.retrbinary('RETR ' + entry.split()[3], lf.write, 4096)
			lf.close()
	f.quit()
	
###

def dwl_feq(station):
	f, feq_files = getFileList('hqcsap')

	for entry in feq_files:
		fname = entry.split()[3][9:-4]
		if fname == station and entry.split()[3].startswith('FeQnyers_'):
			lf = open('%s.feq' % fname, 'wb')
			f.retrbinary('RETR ' + entry.split()[3], lf.write, 4096)
			lf.close()
	f.quit()

###

def dwl_omsz(day):
	f, omsz_files = getFileList('omsz/pufferOMSZ/acttxt/HMP')		
	datefrom = (datetime.date.today() + datetime.timedelta(-day)).strftime('%Y%m%d')	

	for entry in omsz_files:
		fname = entry.split()[3]
		if fname[5:13] >= datefrom and fname.startswith('odHrv2'):
			lf = open('%s' % fname, 'wb')
			f.retrbinary('RETR ' + fname, lf.write, 4096)
			lf.close()
			print '-> %s letoltve...' % fname
	f.quit()
	
###

def dwl_grb(day):

	f, grib_files = getFileList('ovsz')
	
	today = (datetime.date.today()).strftime('%Y%m%d')
	datefrom = (datetime.date.today() + datetime.timedelta(-day)).strftime('%Y%m%d')

	for entry in grib_files:
		fname = entry.split()[3]

		if fname.startswith('gedW') and fname[13:15] == '00':
			fdate = fname[4:12]
			fhour = int(fname[18:21])
			if (fdate >= datefrom and fhour < 25) or fdate == today:
				try:
					lf = open('%s.grb' % fname, 'wb')
					f.retrbinary('RETR ' + fname, lf.write, 4096)
					lf.close()
					print '>> A %s GRIB fajl letoltese megtortent...' % fname
				except:
					print 'HIBA: FTP kapcsolati hiba miatt a letoltes meghiusult, a program leallt.'
					f.quit()
					sys.exit(-1)
	f.quit()

###

def hqcsap2dat(station,day):
	datefrom = (datetime.date.today() + datetime.timedelta(-day)).strftime('%Y.%m.%d')
	
	inp = open('%s.hqc' % station)
	lines = inp.readlines()
	inp.close()

	out = open('%s.dat' % station, 'w')
	out.write('; Merohely: %s \n' % station)
	out.write('; egyeb komment helye\n')
	out.write('15\n')
	writeable = False

	for line in lines:
		if len(line.strip()) == 0: 
			continue
		if line.strip().startswith(datefrom):
			writeable = True
		if writeable:
			fields = line.strip().split(';')
			out.write('%s    %s\n' % (fields[0], fields[1]))

	out.close()

###

def omsz2dat(station,day):
	datefrom = (datetime.date.today() + datetime.timedelta(-day)).strftime('%Y%m%d')			
	filelist = [ f for f in os.listdir(".") if f.endswith(".csv") ]
	
	out = open('%s.dat' % station, 'w')
	out.write('; Merohely: %s \n' % station)
	out.write('; egyeb komment helye\n')
	out.write('60\n')

	for f in filelist:
		inp = open('%s' % f)
		lines = inp.readlines()
		inp.close()

		for line in lines:
			if line.strip().startswith(station):
				fields = line.strip().split(';')
				out.write('%s    %s\n' % (fields[1][:-3], fields[2]))
				break

	out.close()

###

def dat2otp(day):
	datefrom = datetime.date.today() + datetime.timedelta(-day)
	cmd = "precipcomb "+datefrom.strftime('%Y')+" "+datefrom.strftime('%m')+" "+datefrom.strftime('%d')+" 00 00 180"
	os.system(cmd)

###

def otp2dss(day):
	datefrom = (datetime.date.today() + datetime.timedelta(-day)).strftime('%Y%m%d')
	filelist = [ f for f in os.listdir(".") if f.endswith(".otp") ]

	for f in filelist:
		inp = open('%s' % f)
		lines = inp.readlines()
		inp.close()

		n = len(lines)
		data = []

		selector = "/ZALA/%s/PRECIP-INC//3HOUR/OBS/" % f[:-4]
		year  = lines[0].split()[0]
		month = lines[0].split()[1]
		day   = lines[0].split()[2]
		hour  = lines[0].split()[3]
		min   = lines[0].split()[4]
		start = datetime.datetime(int(year),int(month),int(day),int(hour),int(min))
		dt = hours(3)

		for val in range(2,n):
			data.append(lines[val])

		# adathiany miatt az utolsot ismetelni kell:
		data.append(lines[val])

		props = {TIMESTAMP:PERIOD_START,AGGREGATION:"PER-CUM",UNIT:"MM"}

		ts  = rts(data, start, dt, props)
		out = PTH_WORK + 'Zala/HMS/' + DSS_ZALA
		dss_store_ts(ts, out, selector)

		print '>> A %s datumu %s fajl DSS-be toltese megtortent...' % (datefrom, f)

###

def feq2dss(station,day):
	datefrom = (datetime.date.today() + datetime.timedelta(-day)).strftime('%Y.%m.%d')			

	inp = open('%s.feq' % station)
	lines = inp.readlines()
	inp.close()

	selector = "/ZALA/Q-%s/FLOW//IR-DAY/OBS/" % station
	times = []
	data = []

	for line in lines:
		fields = line.strip().split(';')
		datefield = fields[0].split(' ')
		year  = datefield[0].split('.')[0]
		month = datefield[0].split('.')[1]
		day   = datefield[0].split('.')[2]
		hour  = datefield[1].split(':')[0]
		min   = datefield[1].split(':')[1]				
		
		if (datetime.datetime(int(year),int(month),int(day))).strftime('%Y.%m.%d') >= datefrom:
			times.append(ticks(datetime.datetime(int(year),int(month),int(day),int(hour),int(min))))
			data.append(float(fields[1]))
	
	props = {TIMESTAMP:PERIOD_START,AGGREGATION:"INST-VAL",UNIT:"m3/s"}
	ts = its(times, data, props)
	out = PTH_WORK + 'Zala/HMS/' + DSS_ZALA
	dss_store_ts(ts, out, selector)

	print '>> A %s datumu %s fajl DSS-be toltese megtortent...' % (datefrom, station)

###

def grb2db(element):

	meta_tag = element.upper()

	stmt_del = "DELETE FROM ftpadat"
	stmt_sel = "SELECT id FROM ftpadat WHERE easting=? AND northing=? AND element=? AND timestamp=?"
	stmt_ins = "INSERT INTO ftpadat (easting, northing, element, data, timestamp) VALUES (?,?,?,?,?)"
	stmt_upd = "UPDATE ftpadat SET data=? WHERE id=?"

	gdal.UseExceptions()

	db  = sqldb.connect('%sFTP2DSS/ftpimport.sqlite' % PTH_WORK)
	cur = db.cursor()

	filelist = [ f for f in os.listdir(".") if f.endswith(".grb") ]

	for f in filelist:

		# a gedWYYYYMMDD_xx00+HHH formatumban szereplo idopontok atalakitasa
		chour = int(f[18:21])
		ctime = datetime.datetime.strptime(f[4:17],'%Y%m%d_%H%M')
		ctime += datetime.timedelta(hours=chour)
		ctime = datetime.datetime.strftime(ctime,'%Y%m%d%H')

		ds = gdal.Open(f)
		if ds is None:
			print 'HIBA: A %s adatforras nem nyithato meg, a program leallt.' % f
			sys.exit(-1)

		bands = ds.RasterCount
		cols  = ds.RasterXSize
		rows  = ds.RasterYSize

		gt = ds.GetGeoTransform()

		Easting  = gt[0]
		Northing = gt[3]
		eastStep = gt[1]
		northStep= gt[5]

		Easting += eastStep / 2
		Northing += northStep / 2

		for band in range(1, bands+1):
			srcband  = ds.GetRasterBand(band)

			if srcband.GetMetadata()['GRIB_ELEMENT'] == meta_tag:
				data = srcband.ReadAsArray(0, 0, cols, rows)

				for row in range(rows):

					for col in range(cols):
						value = data[row, col]
						# -- vizsgalati teruletre fokuszalas (Balaton, Zala):
						if (16.2 < Easting < 18.3) and (46.2 < Northing < 47.3):
							result = cur.execute(stmt_sel, (Easting, Northing, meta_tag, ctime))
							record = cur.fetchone()

							if record is None:
								cur.execute(stmt_ins, (Easting, Northing, meta_tag, value, ctime))
							else:
								cur.execute(stmt_upd, (value, record[0]))

						Easting += eastStep

					Northing += northStep
					Easting  = gt[0] + (eastStep / 2)

		db.commit()
		ds = None

		print '>> A %s fajl SQLite adatbazisba toltve...' % f

	db.close()

###

def db2dss(watershed,element,day):

	today = (datetime.date.today()).strftime('%Y%m%d')
	tfour = (datetime.date.today() + datetime.timedelta(4)).strftime('%Y%m%d')

	if day > 0:
		datefrom = (datetime.date.today() + datetime.timedelta(-day)).strftime('%Y%m%d')
	else:
		datefrom = today

	if watershed == 'Balaton':
		wsn = 'BALATON'
		out = PTH_WORK + 'Balaton/HMS/' + DSS_BTON
		qry = "SELECT * FROM "+element+"_balaton WHERE date >= '" + datefrom + "'"
	elif watershed == 'Zala':
		wsn = 'ZALA'
		out = PTH_WORK + 'Zala/HMS/' + DSS_ZALA
		qry = "SELECT * FROM "+element+"_zala WHERE date >= '" + today + "'"
	else:
		print "Hibas parameter: " % watershed
		sys.exit(-1)

	loc = 0
	r = []

	db = sqldb.connect('%sFTP2DSS/ftpimport.sqlite' % PTH_WORK)
	cur = db.cursor()

	for t in cur.execute(qry):
		r.append(t)
	cur.close()
	db.close()

	for i in range(len(r)):

		if loc <> r[i][0]:

			if loc <> 0:
				props = {TIMESTAMP:PERIOD_START,AGGREGATION:"PER-CUM",UNIT:"MM"}
				ts = rts(data, start, dt, props)
				dss_store_ts(ts, out, selector)

			loc      = r[i][0]
			selector = "/%s/%s/PRECIP-INC//3HOUR/OBS/" % (wsn, str(loc))
			dt       = hours(3)
			data     = []
			year     = r[i][1][:4]
			month    = r[i][1][4:6]
			day      = r[i][1][6:8]
			start    = datetime.datetime(int(year),int(month),int(day),int(r[i][2]),0)

		if r[i][2] == '00' and r[i][1] == datefrom:
			continue
		if r[i][2] == '03' and r[i][1] <= today:
			data.append(float(r[i][3]))
		else:
			precip = r[i][3]-r[i-1][3]
			if precip < 0:
				precip = 0.000
			if r[i][1] >= tfour and r[i][2] in ('06','12','18'):
				data.append(float(precip / 2))
			data.append(float(precip))
			if r[i][1] >= tfour and r[i][2] == '18':
				precip = r[i+1][3]-r[i][3]
				if precip < 0:
					precip = 0.000
				data.append(float(precip / 2))
				
	props = {TIMESTAMP:PERIOD_START,AGGREGATION:"PER-CUM",UNIT:"MM"}
	ts = rts(data, start, dt, props)
	dss_store_ts(ts, out, selector)
	print '>> Az adatokat a %s DSS-be toltottem...' % watershed

###

def cleaning():
	print '>> TEMP mappa takaritas...'

	filelist = [ f for f in os.listdir(".") if f.endswith(".dat") ]
	for f in filelist:
		os.remove(f)

	filelist = [ f for f in os.listdir(".") if f.endswith(".otp") ]
	for f in filelist:
		os.remove(f)

	filelist = [ f for f in os.listdir(".") if f.endswith(".csv") ]
	for f in filelist:
		os.remove(f)
		
	filelist = [ f for f in os.listdir(".") if f.endswith(".feq") ]
	for f in filelist:
		os.remove(f)
		
	filelist = [ f for f in os.listdir(".") if f.endswith(".hqc") ]
	for f in filelist:
		os.remove(f)
		
	filelist = [ f for f in os.listdir(".") if f.endswith(".grb") ]
	for f in filelist:
		os.remove(f)
		
# --- Main --------------------------------------------------------------------

if len(sys.argv) > 1:
	delta = sys.argv[1]
else:
	delta = 1

os.chdir(PTH_WORK+'temp/')

cleaning()

# Multbeli adatok a Zalara
for f1 in ('AAV764','AAV776','ABE907','ABF066'):
	dwl_hqc(f1)
	print '-> %s letoltve...' % f1
	hqcsap2dat(f1, int(delta))
	
for f2 in ('AAV760','AAV763','AAV774','AAV775','AAV799','ADG334'):
	dwl_feq(f2)
	print '-> %s letoltve...' % f2
	feq2dss(f2, int(delta))

dwl_omsz(int(delta))	
omsz2dat('26118', int(delta))
	
dat2otp(int(delta))
otp2dss(int(delta))

# Elorejelzesi adatok Zalara, Balatonra
dwl_grb(int(delta))

grb2db('tp')
db2dss('Balaton','tp',int(delta))
db2dss('Zala','tp',0)

print '**** VEGE ****'

