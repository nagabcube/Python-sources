# ********************************************************
# DSS adatbetoltes FTP-rol
# ver.: 0.6 - 2015.04.10 - nagabcube
# --------------------------------------------------------

import sys, os, datetime, string, fpformat

from hec.script import *

from hec.io import *
from hec.heclib.dss import *
from hec.heclib.util import *
from hec.hecmath import *

from ftplib import FTP
from urllib2 import urlopen
from time import strftime

from com.ziclix.python.sql import zxJDBC

# -- kornyezeti valtozok, ezek modosithatok

PTH_SQDB = 'D:/work/FTP2DSS/'
PTH_ZALA = 'D:/work/Zala/HMS/'
PTH_BTON = 'D:/work/Balaton/HMS/'

DSS_BTON = 'Balaton-2013.dss'
DSS_ZALA = 'zala.dss'

# -- ftp kapcsolatok

FTP_HOST = 'ftp.ovf.hu'
FTP_USER = 'ftp-user'
FTP_PASS = 'ftp-pwd'

# -- sqlite jdbc beallitasok

DATABASE = 'ftpimport.sqlite'
JDBC_URL = 'jdbc:sqlite:%s%s' % (PTH_SQDB, DATABASE)
JDBC_DRV = 'org.sqlite.JDBC'

ASC_LIST = []
DAY_FROM = datetime.date.today()
MON_ENUM = {'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}

# -- Eljarasok, funkciok, osztalyok

def setConnection(jdbc_url, driverName):
    try:
        dbConn = zxJDBC.connect(jdbc_url, None, None, driverName)
    except zxJDBC.DatabaseError, msg:
        print msg
        sys.exit(-1)

    return dbConn

###

def dwl_hqcsap() :
	
	# -- T-1 nap
	datefrom = (DAY_FROM + datetime.timedelta(-1)).strftime('%Y.%m.%d')
	
	ftp_wksp = 'hqcsap'
	entries  = []
	lstfiles = []
	
	f = FTP(FTP_HOST, FTP_USER, FTP_PASS)
	f.cwd(ftp_wksp)
	f.dir(entries.append)
	
	for entry in entries:
		fname = entry.split()[3][12:-4]
		if fname in ('AAV764','AAV776','ABE907','ABF066') and entry.split()[3].startswith('HmCsapnyers_'):
			lf = open('%s.txt' % fname, "wb")
			f.retrbinary('RETR ' + entry.split()[3], lf.write, 4096)
			lf.close()
			lstfiles.append(fname)
	f.close()

	for station in lstfiles:
		inp = open('%s.txt' % station)
		lines = inp.readlines()
		inp.close()
		
		out = open('%s.dat' % station, 'w')
		out.write('; Merohely: %s \n' % station)
		out.write('; egyéb komment helye\n')
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
		os.remove('%s.txt' % station)
		
###

def dwl_omsz(vor_id) :
	
	# -- T-1 nap
	datefrom = (DAY_FROM + datetime.timedelta(-1)).strftime('%Y%m%d')
	
	ftp_wksp = 'omsz/pufferOMSZ/acttxt/HMP'
	entries  = []
	lstfiles = []
	
	f = FTP(FTP_HOST, FTP_USER, FTP_PASS)
	f.cwd(ftp_wksp)
	f.dir(entries.append)
	
	for entry in entries:
		fname = entry.split()[3]
		if fname[5:-9] == datefrom:
			lf = open('%s' % fname, 'wb')
			f.retrbinary('RETR ' + fname, lf.write, 4096)
			lf.close()
			lstfiles.append(fname)
	f.close()

	out = open('%s.dat' % vor_id, 'w')
	out.write('; Merohely: %s \n' % vor_id)
	out.write('; egyéb komment helye\n')
	out.write('60\n')
	
	for station in lstfiles:
		if len(station) == 22:
			inp = open('%s' % station)
			lines = inp.readlines()
			inp.close()

			for line in lines:
				if line.strip().startswith(vor_id):
					fields = line.strip().split(';')
					out.write('%s    %s\n' % (fields[1][:-3], fields[2]))
					break
					
	out.close()
		
###

def dat2otp():
	
	# -- T-1 nap
	datefrom = DAY_FROM + datetime.timedelta(-1)
	cmd = "precipcomb " + datefrom.strftime('%Y')+" "+datefrom.strftime('%m')+" "+datefrom.strftime('%d')+" 00 00 180"
	os.system(cmd)

###

def otp2dss():
	
	# -- T-1 nap
	datefrom = (DAY_FROM + datetime.timedelta(-1)).strftime('%Y%m%d')
	
	filelist = [ f for f in os.listdir(".") if f.endswith(".otp") ]
	
	for f in filelist:
		inp = open('%s' % f)
		lines = inp.readlines()
		inp.close()
		
		tsc = TimeSeriesContainer()
		tsc.fullName = "/ZALA/%s/PRECIP-INC//3HOUR/OBS/" % f[:-4]
		tsc.interval = 180

		date_value = datefrom[-2:]+MON_ENUM[datefrom[4:-2]]+datefrom[:4]+" 0000"
		hecTime = HecTime(date_value)		

		times = []
		vals  = []
		lnum  = 0
	
		for line in lines:
			lnum += 1
			if lnum >= 3:		
				vals.append(float(line))
		
		# -- meg egyet hozza kell adni:
		vals.append(0.00)
		
		for value in vals:
			times.append(hecTime.value())
			hecTime.add(tsc.interval)
			
		tsc.times = times
		tsc.values = vals
		tsc.numberValues = len(vals)
		tsc.units = "MM"
		tsc.type = "PER-CUM"
		
		fDss = HecDss.open("%s%s" % (PTH_ZALA, DSS_ZALA))
		fDss.put(tsc)
		fDss.close()

###

def dwl_feq():
	
	# -- T-1 nap
	datefrom = (DAY_FROM + datetime.timedelta(-1)).strftime('%Y.%m.%d')

	ftp_wksp = 'hqcsap'
	entries  = []
	lstfiles = []
	times    = []
	vals     = []
	cnt      = 0
	
	f = FTP(FTP_HOST, FTP_USER, FTP_PASS)
	f.cwd(ftp_wksp)
	f.dir(entries.append)
	
	for entry in entries:
		fname = entry.split()[3][9:-4]
		if fname in ('AAO588','AAV760','AAV763','AAV774','AAV775','AAV799','ADG334') and entry.split()[3].startswith('FeQnyers_'):
			lf = open('%s.txt' % fname, 'wb')
			f.retrbinary('RETR ' + entry.split()[3], lf.write, 4096)
			lf.close()
			lstfiles.append(fname)
	f.close()

	tsc  = TimeSeriesContainer()
	tsc.interval = -1
		
	for station in lstfiles:
		inp = open('%s.txt' % station)
		lines = inp.readlines()
		inp.close()
		os.remove('%s.txt' % station)
		
		tsc.fullName = "/ZALA/Q-%s/FLOW//IR-DAY/OBS/" % station
		
		for line in lines:
			if line.strip().startswith(datefrom):
				fields = line.strip().split(';')
				datefield = fields[0].split(' ')
				date_value = datefield[0][-2:]+MON_ENUM[datefield[0][5:-3]]+datefield[0][:4]+' '+datefield[1]
				
				cnt += 1
				
				vals.append(float(fields[1]))
				hecTime = HecTime(date_value)
				times.append(hecTime.value())

			else:
				if cnt > 1:
					cnt = 0
					tsc.times = times
					tsc.values = vals
					tsc.numberValues = len(vals)
				
					tsc.units = "m3/s"
					tsc.type = "INST-VAL"
					tsc.startTime = times[0]
					tsc.endTime = times[-1]
					fDss = HecDss.open("%s%s" % (PTH_ZALA, DSS_ZALA))
					fDss.put(tsc)
					fDss.close()
					times = []
					vals  = []

###

def dwl_ovsz(fc_type) :

	if fc_type not in ('APCP','EVAP','TMP'):
		print "Nem tamogatott elorejelzesi adattipus:" % fc_type
		sys.exit(-1)

	ftp_wksp = "ovsz"
	entries  = []
	
	try:
		f = FTP(FTP_HOST, FTP_USER, FTP_PASS)
		f.cwd('ovsz')
		f.dir(entries.append)
	except Exception, msg :
		print "Nem tudok kapcsolodni az FTP sitehoz" 
		f.close()
		sys.exit(-1)
	
	for entry in entries:
		fdate = entry.split()[0]
		fname = entry.split()[3]
		
		if fname[-3:] == 'asc' and fname[3:-34] == fc_type and fdate == DAY_FROM.strftime('%m-%d-%y'):
			lf = open('%s%s' % (PTH_SQDB, fname), "wb")
			f.retrbinary("RETR " + fname, lf.write, 2048)
			lf.close()
			ASC_LIST.append(fname)
			
	f.close()

###

def storeInDB():

	stmt_del = "DELETE FROM ftpadat"
	stmt_sel = "SELECT id FROM ftpadat WHERE Easting=? AND Northing=? AND Category=? AND Moment=?"
	stmt_ins = "INSERT INTO ftpadat (Easting, Northing, Category, Stamp, Moment) VALUES (?,?,?,?,?)"
	stmt_upd = "UPDATE ftpadat SET Stamp=? WHERE ID=?"
	
	try:
		conn = setConnection(JDBC_URL, JDBC_DRV)
		curs = conn.cursor()
		curs.execute(stmt_del)
	except:
		print "Adatbazis kapcsolati hiba, program leallitva !"
		sys.exit(-1)
	
	ASC_LIST.sort()
	
	for entry in ASC_LIST:
		colno = 0
		rowno = 0
		fname = entry.split()[0]
		c = fname[3:-34]
		m = fname[-14:-4]
			
		ascfile = open('%s%s' % (PTH_SQDB, fname), "r")

		ncols  = string.atof(string.split(ascfile.readline(), " ")[1])
		nrows  = string.atof(string.split(ascfile.readline(), " ")[1])
		xllcnt = string.atof(string.split(ascfile.readline(), " ")[1])
		yllcnt = string.atof(string.split(ascfile.readline(), " ")[1])
		csize  = string.atof(string.split(ascfile.readline(), " ")[1])
		nodata = string.split(ascfile.readline(), " ")[1]

		curX = xllcnt
		curY = yllcnt + csize * (nrows - 1) # mert nem bal also, hanem bal felso !!!

		for coordinates in ascfile:
			for s in string.split(coordinates, " "):

				e = curX + csize * colno
				n = curY - csize * rowno

				result = curs.executemany(stmt_sel, [e, n, c, m])
				if curs.rowcount == 0:
					result = curs.executemany(stmt_ins, [e, n, c, string.atof(s), m])
				else:
					row = curs.fetchone()
					result = curs.executemany(stmt_upd, [string.atof(s), row[0]])
				
				colno += 1
				if colno == ncols:
					colno = 0
					rowno += 1
					
		ascfile.close()
		os.remove('%s%s' % (PTH_SQDB, fname))
		print " *** '%s' feldolgozva." % fname
		
	curs.close()
	conn.commit()
	conn.close()

###

def db2dss_apcp(watershed):
	
	if watershed == 'Balaton':
		dssqry  = "SELECT * FROM dss_apcp_balaton where date >= ?"
		dssfile = DSS_BTON
		dsspath = PTH_BTON
		wsname  = 'BALATON'
		
	elif watershed == 'Zala':
		dssqry  = "SELECT * FROM dss_apcp_zala where date >= ?"
		dssfile = DSS_ZALA
		dsspath = PTH_ZALA
		wsname  = 'ZALA'
		
	else:
		print "Hibas parameter: " % watershed
		sys.exit(-1)
		
	try:
		fDss = HecDss.open("%s%s" % (dsspath, dssfile))
		tsc  = TimeSeriesContainer()
		tsc.interval = -1
		loc  = 0

		conn = setConnection(JDBC_URL, JDBC_DRV)
		curs = conn.cursor()
		curs.executemany(dssqry, [DAY_FROM.strftime('%Y%m%d')])
		
		for row in curs.fetchall():
			date, hour, measured, cellid = row[:]
			
			if loc <> cellid:
				loc   = cellid
				times = []
				vals  = []
				cnt   = 0
				
				# set new location
				tsc.fullName = "/%s/%s/PRECIP-INC//IR-DAY/OBS/" % (wsname, loc)

			# store section
			
			date_value = date[-2:]+MON_ENUM[date[4:-2]]+date[:4]+" "+hour+"00"
			hecTime = HecTime(date_value)
			
			vals.append(measured)
			times.append(hecTime.value())
			
			cnt += 1

			if cnt == 43: # 12 orasnal ez az ertek 49
				tsc.times = times
				tsc.values = vals
				tsc.numberValues = len(vals)
				
				tsc.units = "MM"
				tsc.type = "PER-CUM"
				
				regtsc = HecMath.createInstance(tsc)
				regtsc = TimeSeriesMath(tsc).transformTimeSeries("3HOUR","","ACC",0)
				fDss.write(regtsc)

	except Exception, e :
		print "Hiba a DSS-be tolteskor: " % e

	fDss.close()
	curs.close()
	conn.close()

###

def potlas_apcp(watershed):

	if watershed == 'Balaton':
		dssqry  = "SELECT * FROM dss_apcp_balaton where date >= ?"
		dssfile = DSS_BTON
		dsspath = PTH_BTON
		wsname  = 'BALATON'
		
	elif watershed == 'Zala':
		dssqry  = "SELECT * FROM dss_apcp_zala where date >= ?"
		dssfile = DSS_ZALA
		dsspath = PTH_ZALA
		wsname  = 'ZALA'
		
	else:
		print "Hibas parameter: " % watershed
		sys.exit(-1)
	
	try:
		fDss = HecDss.open("%s%s" % (dsspath, dssfile))
		tsc  = TimeSeriesContainer()
		loc  = 0

		conn = setConnection(JDBC_URL, JDBC_DRV)
		curs = conn.cursor()
		curs.executemany(dssqry, [DAY_FROM.strftime('%Y%m%d')])
		
		for row in curs.fetchall():
			date, hour, measured, cellid = row[:]
			
			if loc <> cellid:
				loc   = cellid
				times = []
				vals  = []
				cnt   = 0
			
				# set new location
				tsc.fullName = "/%s/%s/PRECIP-INC//3HOUR/OBS/" % (wsname, loc)

			# store section
			date_value = date[-2:]+MON_ENUM[date[4:-2]]+date[:4]+" "+hour+"00"
			hecTime = HecTime(date_value)
			
			vals.append(measured)
			times.append(hecTime.value())
			
			cnt += 1

			if cnt == 8:
				tsc.times = times
				tsc.values = vals
				tsc.numberValues = len(vals)
				
				tsc.units = "MM"
				tsc.type = "PER-CUM"
			
				fDss.put(tsc)

	except Exception, e :
		print "Hiba a DSS-be tolteskor: " % e
			
	fDss.close()
	curs.close()
	conn.close()
	
###

def cleaning():
	
	filelist = [ f for f in os.listdir(".") if f.endswith(".csv") ]
	for f in filelist:
		os.remove(f)
		
	filelist = [ f for f in os.listdir(".") if f.endswith(".dat") ]
	for f in filelist:
		os.remove(f)

	filelist = [ f for f in os.listdir(".") if f.endswith(".otp") ]
	for f in filelist:
		os.remove(f)
		
###
	
# --------------------- Main --------------------------------------

# Zala mert adatok betoltese 
dwl_hqcsap()
dwl_omsz('26118')
dat2otp()
otp2dss()
dwl_feq()

# Elorejelzesi adatok betoltese a Zalara es a Balatonra
dwl_ovsz('APCP')
storeInDB()
db2dss_apcp('Balaton')
db2dss_apcp('Zala')
potlas_apcp('Balaton')
potlas_apcp('Zala')

# Munkaallomanyok torlese
cleaning()

print " *** Vege *** "

