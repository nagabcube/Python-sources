import sys

from ucar.nc2.dataset import NetcdfDataset
from com.ziclix.python.sql import zxJDBC

###
PATH        = "C:/Works/"
FILENAME    = "cedh20140801_0000+00300"
DATABASE    = "forecast.db"
VARIABLE    = "conprec"
JDBC_URL    = "jdbc:sqlite:%s%s"  % (PATH, DATABASE)
JDBC_DRIVER = "org.sqlite.JDBC"
QRY_PRCID   = "SELECT * FROM PrecipCells WHERE PrecipID=?"
DEL_DSS     = "DELETE FROM cdfadat WHERE filename=?"
INS_DSS     = "INSERT INTO cdfadat VALUES(?,?,?)"
###

def getConnection(jdbc_url, driverName):
    try:
        dbConn = zxJDBC.connect(jdbc_url, None, None, driverName)
    except zxJDBC.DatabaseError, msg:
        print msg
        sys.exit(-1)

    return dbConn

###
nc = NetcdfDataset.openDataset("%s%s" % (PATH, FILENAME))
precval = nc.findVariable("%s" % VARIABLE)
cells = precval.read()
nc.close()

shapes = cells.getShape()

dbConn = getConnection(JDBC_URL, JDBC_DRIVER)
cursor = dbConn.cursor()
cursor.execute(DEL_DSS, [FILENAME])

for x in range(shapes[1]):
    for y in range(shapes[2]):
        valID = 1000*y + x
        cursor.execute(QRY_PRCID, [valID])
        if cursor.rowcount > 0:
            valPrec = cells.get(0, x, y)
            cursor.execute(INS_DSS,[valID, FILENAME, valPrec])

dbConn.commit()
dbConn.close()
