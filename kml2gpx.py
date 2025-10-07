#!/usr/bin/python

import argparse
import xml.sax

parser = argparse.ArgumentParser(description='Convert kml data to gpx file')
parser.add_argument('input_file')

args = parser.parse_args()
input = args.input_file

class KmlParser(xml.sax.ContentHandler):
	def __init__(self):
		self.in_tag=0
		self.chars=""
		self.when=""
		self.started=0
	def startElement(self, name, attrs):
		if name == "gx:coord":
			self.in_tag=1
			self.chars=""
		if name == "when":
			self.in_tag=1
			self.chars=""
		if name == "gx:Track":
			if self.started:
				print ("</trkseg>")
				print ("<trkseg>")
	def characters(self, char):
		if self.in_tag:
			self.chars += char
	def endElement(self, name):
		if name == "when":
			self.in_coord=0
			self.when = self.chars
			self.chars=""
		if name == "gx:coord":
			self.in_coord=0
			self.started=1
			coords=self.chars
			self.chars=""
			coords = coords.split()
			print ("\t<trkpt lat=%s lon=%s>" % (coords[1],coords[0]))
			if len(coords)>2:
				print ("\t\t<ele>%s</ele>" % coords[2])
			print ("\t\t<time>%s</time>" % self.when)
			print ("\t</trkpt>")

print ("""<?xml version="1.0" encoding="UTF-8"?>
<gpx version="1.0"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns="http://www.topografix.com/GPX/1/0"
	xsi:schemaLocation="http://www.topografix.com/GPX/1/0 http://www.topografix.com/GPX/1/0/gpx.xsd">
<trk><trkseg>""")

parser = xml.sax.make_parser()
parser.setContentHandler(KmlParser())
parser.parse(open(input,"r"))

print ("""</trkseg></trk>
</gpx>""")