'''
NAME
    NetCDF to DSS
PURPOSE
    a.read data from NetCDF files and write to sqlite database,
    b.reading sqlite table to create DSS file
PROGRAMMER
    nagabcube
REVISION HISTORY
    20140728 -- Initial version created
REFERENCES
    netcdf4-python -- http://code.google.com/p/netcdf4-python/

'''
from netCDF4 import MFDataset
import numpy as np
import sqlite3 as sqdb
import sys

if __name__ == "__main__":

    inputfile = sys.argv[1]

    db  = sqdb.connect('forecasts.db')
    cur = db.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS cdfadat (cellid INTEGER, filename TEXT, csapadek NUMERIC)''')
    cur.execute('''CREATE VIEW IF NOT EXISTS dss_precip AS SELECT substr(cdfadat.filename, 5, 8) AS datum, substr(cdfadat.filename, 14, 4) AS idopont, cdfadat.csapadek/1000 AS Ertek, cdfadat.cellid FROM cdfadat ORDER BY cdfadat.cellid, substr(cdfadat.filename, 5, 8), substr(cdfadat.filename, 14, 4)''')
    
    rootgrp = MFDataset(inputfile, 'r')
    cells   = rootgrp.variables['conprec'][:]
    nx, ny  = cells[0].shape
    grd_arr = np.ravel(cells)
    c = -1
    
    for x in range(nx):
        for y in range(ny):
            c += 1
            if x >= 119 and x < 174:
                if y >= 71 and y < 118:
                    cellid = (1000*(x+1))+y+1
                    cellvalue = grd_arr[c] * 1000
                    cur.execute('''INSERT INTO cdfadat VALUES (:cellid, :filename, :csapadek)''', {'cellid' :cellid, 'filename' :inputfile, 'csapadek' :cellvalue})	

    db.commit()
    db.close()
    rootgrp.close()