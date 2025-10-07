import sys, os, datetime, string
import sqlite3 as db 


# -- globalis kornyezeti valtozok, ezek modosithatok:

PTH_WORK = 'D:/work/'
DSS_BTON = 'Balaton-2013.dss'
DSS_ZALA = 'zala.dss'

conn = db.connect(PTH_WORK + 'FTP2DSS/ftpimport.sqlite')
curs = conn.cursor()

stmt_ins = "INSERT INTO ftpadat (easting, northing, element, data, timestamp) VALUES (?,?,?,?,?)"

os.chdir (PTH_WORK + '/asc')

filelist = [ f for f in os.listdir(".") ]
filelist.sort()

for f in filelist:
    
    colno = 0
    rowno = 0
    c = f[3:-34]
    m = f[-14:-4]
    
    ascfile = open('%s' % f)
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
        
            curs.execute(stmt_ins, (e, n, c, string.atof(s), m))
        
            colno += 1
            
            if colno == ncols:
                colno = 0
                rowno += 1
        
    ascfile.close()
    print " *** '%s' feldolgozva." % f
        
curs.close()
conn.commit()
conn.close()