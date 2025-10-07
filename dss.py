import sys

from hec.script import *
from hec.heclib.dss import *
from hec.heclib.util import *
from hec.io import *
from com.ziclix.python.sql import zxJDBC

###

PATH        = "C:/Works/"
DATABASE    = "forecast.db"
DSS_FILE    = "Balaton-2014.dss"
COUNTER     = 1367
JDBC_URL    = "jdbc:sqlite:%s%s"  % (PATH, DATABASE)
JDBC_DRIVER = "org.sqlite.JDBC"
QRY_PRECIP  = "SELECT * FROM dss_precip"

###

def getConnection(jdbc_url, driverName):
    try:
        dbConn = zxJDBC.connect(jdbc_url, None, None, driverName)
    except zxJDBC.DatabaseError, msg:
        print msg
        sys.exit(-1)

    return dbConn

###

class prettyfloat(float):
    def __repr__(self):
        return "%0.3f" % self

try :
    try :
        myDss  = HecDss.open("%s%s" % (PATH, DSS_FILE))
        tsc    = TimeSeriesContainer()
        loc    = 0
        dbConn = getConnection(JDBC_URL, JDBC_DRIVER)
        stmt   = dbConn.createStatement()
        rSet   = stmt.executeQuery(QRY_PRECIP)
        
        while rSet.next():
            strdate = rSet.getString("datum")
            strtime = rSet.getString("idopont")
            fltval  = rSet.getString("mm")
            intcell = rSet.getString("cella")

            if loc <> intcell:
                loc   = intcell
                vals  = []
                cnt   = 0

            vals.append(fltval)
            cnt += 1

            if cnt == COUNTER:
                start = HecTime("01AUG2014", "0000")
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