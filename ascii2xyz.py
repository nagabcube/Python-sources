#!/usr/bin/python

import sys, gdal

in_file = str(sys.argv[1])

drv = gdal.GetDriverByName('XYZ')
ds_in = gdal.Open('%s' % in_file)
ds_out = drv.CreateCopy('%s.xyz' % in_file, ds_in)
ds_in = None
ds_out = None
