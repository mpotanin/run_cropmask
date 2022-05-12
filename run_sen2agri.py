import re
import argparse
import os
import sys
import re
import shutil
import time
import datetime
import array
import errno
import json
from osgeo import gdal

import resource
import subprocess
import multiprocessing
from psutil import virtual_memory
import multiprocessing, concurrent.futures
from concurrent.futures import ProcessPoolExecutor



from common_utils import raster_proc

# calc values: number of tiles processing in parallel, threads per tile, RAM mb per tile 
def calc_processing_capacity_params (unique_tiles) :
    cpu_count = multiprocessing.cpu_count()
    ram_mb_total = virtual_memory().total // 2**20
    
    tiles_proc = min(8, int(0.55*ram_mb_total) // 4000, unique_tiles)
    
    # (tiles in parallel, threads per tile, RAM mb per tile)
    return ( tiles_proc, (cpu_count // tiles_proc) + 1, int(0.55 * ram_mb_total) // tiles_proc) 
    
    

# calcs tiles num (unique tiles) from scene list
def calcl_unique_tiles (scene_path_list) :
    return len(set([os.path.basename(os.path.dirname(scene_path))[39:44] for scene_path in scene_path_list]))
    
    
# parses input folder and saves pathes to MTD_MSIL2A.xml files into output list
# input scenes maybe filtered by: date interval or tile list
def create_scene_path_list (input_folder, sd = 20000000, ed = 30000000, tile_filter = None) :
    scene_path_list = list()
    
    for scene_id in os.listdir(input_folder):
        
        if not re.match("S2[A-D]_MSIL2A_.+_T([A-Z0-9]+)_.+", scene_id, re.IGNORECASE): continue
        scene_date = int(scene_id[11:19])
        if (scene_date < sd) or (scene_date > ed): continue
        if tile_filter is not None:
            if scene_id[39:44] not in tile_filter: continue
            
        scene_path_list.append(os.path.join(os.path.join(input_folder,scene_id),'MTD_MSIL2A.xml'))

    return scene_path_list
    
    

parser = argparse.ArgumentParser(description='Parses input parameters, create input scene list and launches                                               CropMaskFused.py script')

parser.add_argument('-i', required=True, metavar='input folder', help='Input folder with L2A products (after aws2scihub converter)')
parser.add_argument('-o', required=True, metavar='output folder', help='Output folder')
parser.add_argument('-refp', required=True, metavar='reference polygons', help='Reference polygons')
parser.add_argument('-ntrees', required=False, metavar='number of trees', type=int, default=100, 
                    help='The number of trees used for training')
parser.add_argument('-mtd', required=False, metavar='maximum tree depth', type=int, default=15, 
                    help='Maximum depth of the trees used for Random Forest classifier')
parser.add_argument('-ratio', required=False, metavar='ratio', type=float, default=0.75, 
                    help='The ratio between training and validation polygons') 
parser.add_argument('-rseed', required=False, metavar='random seed', type=int, default=0, 
                    help='Random seed is used to initialize the random number generator') 


if (len(sys.argv)==1):
    parser.print_usage()
    exit(0)

args = parser.parse_args()

sen2agri_script_path = '/home/ubuntu/sen2agri-processors_new/scripts/'
sen2agri_run_script_base = ('python /home/ubuntu/sen2agri-processors_new/scripts/CropMaskFused.py -skip-segmentation -no-red-edge -nbtrsample 500000 -classifier rf -pixsize 10 -rfmin 25 -window 6 -lmbd 2 -eroderad 1 -alpha 0.01 -nbcomp 6 -spatialr 10 -ranger 0.65 -minsize 10 -minarea 20 -main-mission-segmentation SENTINEL -lut /home/ubuntu/dev/run_cropmask/crop-mask.lut')


scene_list = create_scene_path_list(args.i)

if len(scene_list)==0:
    print("Can't find input scenes")
    exit(1)

(tile_proc, tile_threads, tile_ram_mb) = calc_processing_capacity_params(calcl_unique_tiles(scene_list))

resource.setrlimit(resource.RLIMIT_NOFILE, (500000, 500000)) # increase linux limit for simultaneously max opened files
os.environ['OTB_MAX_RAM_HINT'] = str(tile_ram_mb)
os.environ['GDAL_DATA'] = '/usr/share/gdal'
os.environ['PROJ_LIB'] ='/home/ubuntu/OTB-7.1.0-Linux64/share/proj'
os.environ['OTB_APPLICATION_PATH']='/home/ubuntu/OTB-7.1.0-Linux64/lib/otb/applications'
os.environ['PATH']=os.environ['PATH'] + ':/home/ubuntu/OTB-7.1.0-Linux64/bin'


os.chdir(sen2agri_script_path)

input_scenes_str = '-input '
for scene in scene_list:
    input_scenes_str+=scene + ' '

cmd = f'{sen2agri_run_script_base} -ratio {args.ratio} -rfnbtrees {args.ntrees} -rfmax {args.mtd} -max-parallelism {tile_proc} '
cmd +=f'-tile-threads-hint {tile_threads} -refp {args.refp} -outdir {args.o} -rseed {args.rseed}'
cmd +=f'-targetfolder {os.path.join(args.o,"formatted")} -outprops {os.path.join(args.o,"product_properties.txt")} {input_scenes_str}'
#print((tile_proc, tile_threads, tile_ram_mb))
print(cmd)
#exit(0)
os.system(cmd)
 



# create scene list ++
# count tiles, cpu, RAM ++
# calc parameters tiles_in_parallel, threads_for_tile, OTB_MAX_RAM_HINT ++
# set environment variables:  OTB_MAX_RAM_HINT, ulimit, GDAL_DATA

 
