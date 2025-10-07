import sys, os, datetime, string

from ftplib import FTP

# -- globalis kornyezeti valtozok, ezek modosithatok:

PTH_WORK = 'D:/work/'

###

def getFileList(remote_dir):

    entries  = []

    try:
		f = FTP('ftp.ovf.hu', 'ftp-user', 'ftp-pwd')
        f.cwd(remote_dir)
        f.dir(entries.append)
    except Exception, msg :
        print 'HIBA: Nem tudok kapcsolodni az FTP sitehoz, a program leallt.'
        f.quit()
        sys.exit(-1)

    return (f, entries)

###

def dwl_hqcsap(station,day):

    f, hqcsap_files = getFileList('hqcsap')	
    datefrom = (datetime.date.today() + datetime.timedelta(int(-day))).strftime('%Y.%m.%d')

    for entry in hqcsap_files:
        fname = entry.split()[3][12:-4]
        if fname == station and entry.split()[3].startswith('HmCsapnyers_'):
            lf = open('%s.hqc' % fname, 'wb')
            f.retrbinary('RETR ' + entry.split()[3], lf.write, 4096)
            lf.close()
    f.quit()

    filelist = [ f for f in os.listdir(".") if f.endswith(".hqc") ]

    for f in filelist:
        inp = open('%s' % f)
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

    print '>> A %s.dat merohely, %s idosorral letoltve...' % (station, datefrom)

###

if len(sys.argv) > 1:
    delta = sys.argv[1]
else:
    delta = 0

os.chdir(PTH_WORK+'temp/')

for cnt in range(int(delta),-1,-1):
    cleaning()
    #
    # Zala multbeli adatok betoltese
    #
    if cnt > 0:
        dwl_hqcsap('AAV764',cnt)
        dwl_hqcsap('AAV776',cnt)
        dwl_hqcsap('ABE907',cnt)

        dwl_hqcsap('ABF066',cnt)
