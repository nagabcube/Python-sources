# name=sqlite3-dss
# displayinmenu=true
# displaytouser=true
# displayinselector=true
from hec.script import *
from hec.heclib.dss import *
from hec.heclib.util import *
from hec.io import *

import java
import sys

from java.lang import Class
from java.sql  import DriverManager, SQLException

###

DATABASE    = "W:/Balaton/CDF/netcdf_import.db"
JDBC_URL    = "jdbc:sqlite:%s"  % DATABASE
JDBC_DRIVER = "org.sqlite.JDBC"
TBL_QUERY   = '''SELECT * FROM dss_precip'''

###

def getConnection(jdbc_url, driverName):
    try:
        Class.forName(driverName).newInstance()
    except Exception, msg:
        print msg
        sys.exit(-1)

    try:
        dbConn = DriverManager.getConnection(jdbc_url)
    except SQLException, msg:
        print msg
        sys.exit(-1)

    return dbConn

###
	
class prettyfloat(float):
    def __repr__(self):
        return "%0.3f" % self

try :
    try :
        myDss  = HecDss.open("W:/Balaton/CDF/Balaton.dss")
        tsc    = TimeSeriesContainer()
        loc    = 0
        dbConn = getConnection(JDBC_URL, JDBC_DRIVER)
        stmt   = dbConn.createStatement()
        rSet   = stmt.executeQuery(TBL_QUERY)
        
        while rSet.next():
            strdate = rSet.getString("datum")
            strtime = rSet.getString("idopont")
            fltval  = rSet.getString("Ertek")
            intcell = rSet.getString("cellid")
            
            if loc <> intcell:
                loc   = intcell
                vals  = []
                cnt   = 0
                
            vals.append(fltval)
            cnt += 1
            
            if cnt == 192:
                start = HecTime("29MAR2013", "0000")
                tsc.fullName = "/BALATON/%s/PRECIP-INC//1HOUR/OBS/" % loc
                tsc.interval = 60
                precips = map(prettyfloat, vals)
                times = []

                for value in precips:
                    times.append(start.value())
                    start.add(tsc.interval)
                        
                tsc.times = times
                tsc.values = precips
                tsc.numberValues = len(precips)
                tsc.units = "MM"
                tsc.type = "PER-CUM"
                myDss.put(tsc)
    
    except Exception, e :
        MessageBox.showError(' '.join(e.args), "Python Error")
    except java.lang.Exception, e :
        MessageBox.showError(e.getMessage(), "Error")

finally :
    stmt.close()
    dbConn.close()
