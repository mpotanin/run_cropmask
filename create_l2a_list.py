import argparse
import os
import sys
import re

parser = argparse.ArgumentParser(description = 'Parse input folder and prints proper file paths (*MTD_MSIL2A.xml or *_MTD_ALL.xml) to output')

parser.add_argument('-i',required=True, metavar='input folder', help='Input folder with L2 products')
parser.add_argument('-t', required=False, metavar='tiles filter', help='List of tiles comma separated')
parser.add_argument('-p', required=False, metavar='maja | sen2cor', default='sen2cor', 
                    help='List of tiles comma separated without T')
parser.add_argument('-sd', required=False, type=int, default=0, metavar='start yyyymmdd',help='Start date yyyymmdd')
parser.add_argument('-ed', required=False, type=int, default=30000000, metavar='end yyyymmdd', help='End date yyyymmdd')


if (len(sys.argv)==1):
    parser.print_usage()
    exit(0)

args = parser.parse_args()

tiles = None
if args.t is not None:
    tiles = args.t.upper().split(',')
    

#for d in os.listdir(args.i):


#text_file = open(args.o,'w')
if args.p.lower()=='maja':
    for (dirpath, dirnames, filenames) in os.walk(args.i):
        if (os.path.basename(dirpath).startswith('L2NOTV')): continue
        
        for file in filenames:
            if (file.endswith('_MTD_ALL.xml')):
              #SENTINEL2A_20180626-075138-003_L2A_T39UVB_C_V1-0_MTD_ALL.xml
                img_date = int(file[11:19])
                if (img_date <args.sd) or (img_date > args.ed): break
              
                if tiles is not None:
                    if tiles.count(file[36:41]) == 0: break
                                        
                print(os.path.join(dirpath,file)+' ',end='')
else:
    for d in os.listdir(args.i):
        if re.match("S2[A-D]_MSIL2A_.+_T([A-Z0-9]+)_.+", d, re.IGNORECASE):
        #if d.startswith('S2A_MSIL2A_'):
            img_date = int(d[11:19])
            if (img_date <args.sd) or (img_date > args.ed): continue
            
            if tiles is not None:
                    if tiles.count(d[39:44]) == 0: continue
            
            print (os.path.join(os.path.join(args.i,d),'MTD_MSIL2A.xml') + ' ',end='')
            
#S2A_MSIL2A_20190601T074611_N0212_R135_T39UVB_20190601T100823.SAFE


            #text_file.write(os.path.join(dirpath,file)+' ')
            #text_file.close()
