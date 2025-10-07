import sys, os, datetime, string
import gdal
import sqlite3 as sqldb 

from ftplib import FTP
from time import strftime

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
		f = FTP('ftp.ovf.hu', 'vitukih.modellad.ftp', 'Vm20150207')
		f.cwd(remote_dir)
		f.dir(entries.append)
	except Exception, msg :
		print 'HIBA: Nem tudok kapcsolodni az FTP sitehoz, a program leallt.'
		f.close()
		sys.exit(-1)
	
	return (f, entries)
	
###

def dwl_hqcsap(station,day):
	
	f, hqcsap_files = getFileList('hqcsap')	
	datefrom = (datetime.date.today() + datetime.timedelta(int(-day))).strftime('%Y.%m.%d')
	
	localfiles = []
	
	for entry in hqcsap_files:
		fname = entry.split()[3][12:-4]
		if fname == station and entry.split()[3].startswith('HmCsapnyers_'):
			lf = open('%s.txt' % fname, 'wb')
			f.retrbinary('RETR ' + entry.split()[3], lf.write, 4096)
			lf.close()
			localfiles.append(fname)
	f.close()

	for station in localfiles:
		inp = open('%s.txt' % station)
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
		os.remove('%s.txt' % station)

		print '>> A %s datumu %s merohely bemeneti allomanya elkeszult...' % (datefrom, station)

###

def dwl_omsz(station,day):

	f, omsz_files = getFileList('omsz/pufferOMSZ/acttxt/HMP')		
	datefrom = (datetime.date.today() + datetime.timedelta(int(-day))).strftime('%Y%m%d')	

	localfiles = []
	
	for entry in omsz_files:
		fname = entry.split()[3]
		if fname[5:-9] == datefrom:
			lf = open('%s' % fname, 'wb')
			f.retrbinary('RETR ' + fname, lf.write, 4096)
			lf.close()
			localfiles.append(fname)
	f.close()

	out = open('%s.dat' % station, 'w')
	out.write('; Merohely: %s \n' % station)
	out.write('; egyeb komment helye\n')
	out.write('60\n')
	
	for f in localfiles:
		if len(f) == 22:
			inp = open('%s' % f)
			lines = inp.readlines()
			inp.close()

			for line in lines:
				if line.strip().startswith(station):
					fields = line.strip().split(';')
					out.write('%s    %s\n' % (fields[1][:-3], fields[2]))
					break

	out.close()
	
	print '>> A %s datumu %s merohely bemeneti allomanya elkeszult...' % (datefrom, station)

###

def dwl_feq(station,day):
	
	f, feq_files = getFileList('hqcsap')
	datefrom = (datetime.date.today() + datetime.timedelta(int(-day))).strftime('%Y.%m.%d')

	localfiles = []
	
	for entry in feq_files:
		fname = entry.split()[3][9:-4]
		if fname == station and entry.split()[3].startswith('FeQnyers_'):
			lf = open('%s.txt' % fname, 'wb')
			f.retrbinary('RETR ' + entry.split()[3], lf.write, 4096)
			lf.close()
			localfiles.append(fname)
	f.close()

	for station in localfiles:
		inp = open('%s.txt' % station)
		lines = inp.readlines()
		inp.close()
		
		selector = "/ZALA/Q-%s/FLOW//IR-DAY/OBS/" % station
		times = []
		data = []
		cnt = 0
		
		for line in lines:
			if line.strip().startswith(datefrom):
				fields = line.strip().split(';')
				datefield = fields[0].split(' ')
				year  = datefield[0].split('.')[0]
				month = datefield[0].split('.')[1]
				day   = datefield[0].split('.')[2]
				hour  = datefield[1].split(':')[0]
				min   = datefield[1].split(':')[1]				
				cnt += 1

				times.append(ticks(datetime.datetime(int(year),int(month),int(day),int(hour),int(min))))
				data.append(float(fields[1]))
			else:
				if cnt > 0:
					props = {TIMESTAMP:PERIOD_START,AGGREGATION:"PER-CUM",UNIT:"MM"}
					ts = its(times, data, props)
					out = PTH_WORK + '/Zala/HMS/' + DSS_ZALA
					dss_store_ts(ts, out, selector)
					
					print '>> A %s datumu %s fajl DSS-be toltese megtortent...' % (datefrom, station)

###

def dat2otp(day):
	
	datefrom = datetime.date.today() + datetime.timedelta(int(-day))
	cmd = "precipcomb "+datefrom.strftime('%Y')+" "+datefrom.strftime('%m')+" "+datefrom.strftime('%d')+" 00 00 180"
	os.system(cmd)

###

def otp2dss(day):

	datefrom = (datetime.date.today() + datetime.timedelta(int(-day))).strftime('%Y%m%d')
	filelist = [ f for f in os.listdir(".") if f.endswith(".otp") ]
	
	for f in filelist:
		inp = open('%s' % f)
		lines = inp.readlines()
		inp.close()

		n = len(lines) - 1
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
		
		ts = rts(data, start, dt, props)

		out = PTH_WORK + '/Zala/HMS/' + DSS_ZALA
		dss_store_ts(ts, out, selector)
		
		print '>> A %s datumu %s fajl %s-be toltese megtortent...' % (datefrom, f, out)
		
###

def cleaning():
	
	print '>> TMP mappa takaritas...'
	
	filelist = [ f for f in os.listdir(".") if f.endswith(".csv") ]
	for f in filelist:
		os.remove(f)
		
	filelist = [ f for f in os.listdir(".") if f.endswith(".dat") ]
	for f in filelist:
		os.remove(f)

	filelist = [ f for f in os.listdir(".") if f.endswith(".otp") ]
	for f in filelist:
		os.remove(f)

	filelist = [ f for f in os.listdir(".") if f.endswith(".txt") ]
	for f in filelist:
		os.remove(f)
	
	filelist = [ f for f in os.listdir(".") if f.endswith(".grb") ]
	for f in filelist:
		os.remove(f)
		
###

def getGrib(element,day):
	
	if day > 0:
		datefrom = (datetime.date.today() + datetime.timedelta(int(-day))).strftime('%Y%m%d')
		# -- ha T-day napok, akkor csak az elso 24 ora fajljait (8 db.) kell letolteni
		all = False
	else:
		datefrom = (datetime.date.today()).strftime('%Y%m%d')
		all = True
	
	f, grb_files = getFileList('ovsz')
	
	cnt = 0
	meta_element = element.upper()
	
	for entry in grb_files:
		fname = entry.split()[3]
		if fname[4:17] == datefrom + '_0000' and fname.startswith('gedW'):
			lf = open('%s.grb' % fname, 'wb')
			f.retrbinary('RETR ' + fname, lf.write, 4096)
			lf.close()
			print '>> A %s GRIB fajl letoltese megtortent...' % fname
			cnt += 1
			if cnt > 7 and not all:
				break
	f.close()
    
 	stmt_del = "DELETE FROM ftpadat"
	stmt_sel = "SELECT id FROM ftpadat WHERE easting=? AND northing=? AND element=? AND timestamp=?"
	stmt_ins = "INSERT INTO ftpadat (easting, northing, element, data, timestamp) VALUES (?,?,?,?,?)"
	stmt_upd = "UPDATE ftpadat SET data=? WHERE ID=?"
	
	filelist = [ f for f in os.listdir(".") if f.endswith(".grb") ]

	gdal.UseExceptions()
	gdal.PushErrorHandler('CPLQuietErrorHandler')
	
	for f in filelist:
		
		ds = gdal.Open(f)
		if ds is None:
			print 'HIBA: %s fajl nem letezik, a program leallt.' % f
			sys.exit(-1)
		
		# a gedWYYYYMMDD_0000+HHH formatumban szereplo idopontok atalakitasa
		chour = int(f[-7:-4])
		ctime = datetime.datetime.strptime(f[4:17],'%Y%m%d_%H%M')
		ctime += datetime.timedelta(hours=chour)
		ctime = datetime.datetime.strftime(ctime,'%Y%m%d%H')
		
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
		
		db = sqldb.connect('%sFTP2DSS/ftpimport.sqlite' % PTH_WORK)
		cur = db.cursor()
		
		for band in range(1, bands+1):
			srcband  = ds.GetRasterBand(band)

			if srcband.GetMetadata()['GRIB_ELEMENT'] == meta_element:
				data = srcband.ReadAsArray(0, 0, cols, rows)

				for row in range(rows):

					for col in range(cols):
						value = data[row, col]
						# -- csak a 3 oras elorejelzeseket tarolom most
						if chour < 97:
							result = cur.execute(stmt_sel, (Easting, Northing, meta_element, ctime))
							record = cur.fetchone()
							if record is None:
								cur.execute(stmt_ins, (Easting, Northing, meta_element, value, ctime))
							else:
								cur.execute(stmt_upd, (value, record[0]))

						Easting += eastStep

					Northing += northStep
					Easting  = gt[0]
					Easting += eastStep / 2

		db.commit()
		db.close()

		ds = None
		
		print '>> A %s fajl SQLite adatbazisba toltese megtortent...' % (f)
		
###

def db2dss(watershed,element,day):
	
	if watershed == 'Balaton':
		wsname  = 'BALATON'
		dssqry  = "SELECT * FROM dss_"+element+"_balaton where date>="
		out = PTH_WORK + 'Balaton/HMS/' + DSS_BTON
		
	elif watershed == 'Zala':
		wsname  = 'ZALA'
		dssqry  = "SELECT * FROM dss_"+element+"_zala where date>="
		out = PTH_WORK + 'Zala/HMS/' + DSS_ZALA
		
	else:
		print "Hibas parameter: " % watershed
		sys.exit(-1)
		
	if day > 0:
		datefrom = (datetime.date.today() + datetime.timedelta(int(-day))).strftime('%Y%m%d')
		cnt = 8
	else:
		datefrom = (datetime.date.today()).strftime('%Y%m%d')
		cnt = 54

	db = sqldb.connect('%sFTP2DSS/ftpimport.sqlite' % PTH_WORK)
	cur = db.cursor()
	cur.execute(dssqry+str(datefrom))

	loc = 0

	for row in cur.fetchall():
		date, hour, measured, cellid = row[:]
				
		if loc <> cellid:

			loc   = cellid
			data  = []
			year  = date[:4]
			month = date[4:6]
			day   = date[6:8]
			start = datetime.datetime(int(year),int(month),int(day),int(hour),0)
			dt    = hours(3)
			num   = 0
			
			selector = "/%s/%s/PRECIP-INC//3HOUR/OBS/" % (wsname, str(loc))

		data.append(float(measured))
		
		num += 1
		
		if num == cnt:
			props = {TIMESTAMP:PERIOD_START,AGGREGATION:"PER-CUM",UNIT:"MM"}
			ts = rts(data, start, dt, props)
			dss_store_ts(ts, out, selector)
			print '>> %s DSS-be toltve...' % selector		

	db.close()

# --- Main --------------------------------------------------------------------

if len(sys.argv) > 1:
	delta = sys.argv[1]
else:
	delta = 0

os.chdir(PTH_WORK+'temp/')

for cnt in range(int(delta),-1,-1):
	cleaning()
	# Zala multbeli adatok betoltese
	dwl_hqcsap('AAV764',cnt)
	dwl_hqcsap('AAV776',cnt)
	dwl_hqcsap('ABE907',cnt)
	dwl_hqcsap('ABF066',cnt)
	dwl_omsz('26118',cnt)
    #
	dwl_feq('AAV760',cnt)
	dwl_feq('AAV763',cnt)
	dwl_feq('AAV774',cnt)
	dwl_feq('AAV775',cnt)
	dwl_feq('AAV799',cnt)
	dwl_feq('ADG334',cnt)
    #
	dat2otp(cnt)
	otp2dss(cnt)
	# Balaton, Zala elorejelzesi adatok betoltese
	getGrib('tp',cnt)
	db2dss('Balaton','tp',cnt)

# A Zalara csak a mai kell, a tobbi multbeli
db2dss_apcp('Zala','tp',0)

print '*** Vege ***'

	