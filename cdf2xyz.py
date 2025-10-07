'''
NAME
    NetCDF to XYZ
PURPOSE
    a.read data from NetCDF files and write to XYZ text,
PROGRAMMER
    nagabcube
REVISION HISTORY
    20140728 -- Initial version created
REFERENCES
    netcdf4-python -- http://code.google.com/p/netcdf4-python/

'''
from netCDF4 import MFDataset
import sys

if __name__ == "__main__":
	inputfile = sys.argv[1]
	
	ncf = MFDataset(inputfile, 'r')
	lat = ncf.variables['La1'][:]
	lon = ncf.variables['Lo1'][:]
	dx  = ncf.variables['Dx'][:]
	dy  = ncf.variables['Dy'][:]
	nx  = ncf.variables['Nx'][:]
	ny  = ncf.variables['Ny'][:]
	c   = -1

	print "CellID Lon Lat"	

	for x in range(nx):
		for y in range(ny):
			c += 1
			cellid = (1000*(x+1))+y+1
                        print "%i %.3f %.3f" % (cellid, lon+x*dx, lat-y*dy)
	
	ncf.close()
