import argparse
import os
import sys
import re
import argparse
import os
import sys
import re
import shutil
import time
import datetime
import array
import subprocess
import errno
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
from common_utils import raster_proc

# This script prepares S2-L2 scenes  downloaded from AWS to be processed with 
# sen2agri console utils. Two operations are fulfilled: 
# 1) aws to scihub S2-L2 converter: subfolder structures and filenames
# 2) jp2 to tiff converter (using gdal_translate command line util)


# Converts S2-L2 scene folder (nested folders structure and filenames) 
# from AWS format to Sci-HUB format  


def aws2schihub (scene_full_path):

    scene = os.path.basename(scene_full_path)    
    
    # metadata.xml -> MTD_MSIL2A.xml
    os.rename(os.path.join(scene_full_path,'metadata.xml'),os.path.join(scene_full_path,'MTD_MSIL2A.xml'))
    # productInfo.json isn't needed
    os.remove(os.path.join(scene_full_path,'productInfo.json'))
              
              
    # it's rare but is possible that there are two version inside scene folder: "0" and "1"
    # if it is the case then one of them is removed and the rest is converted
    # by default there is only "0" version
    # if subfolder "1" exists we've to decide 
    # which version of scene ("0" or "1") to delete.
    # For that purpose we extract 'name' from 0/productInfo.json
    # if extracted name equals scene then "0" is converted and "1" is deleted
    # other way vice versa
    sub_scene_path = os.path.join(scene_full_path,'0')
    sub_scene_to_delete = None
    if os.path.exists(os.path.join(scene_full_path,'1')) :
        product_info_file = open(os.path.join(sub_scene_path,'productInfo.json'))
        data = json.load(product_info_file)
        if (data['name'] == scene) :
             sub_scene_to_delete = os.path.join(scene_full_path,'1')
        else:
            sub_scene_to_delete = sub_scene_path = os.path.join(scene_full_path,'0')
            sub_scene_path = os.path.join(scene_full_path,'1')
        product_info_file.close()
    # delete second version of scene
    if sub_scene_to_delete is not None:
        shutil.rmtree(os.path.join(scene_full_path,sub_scene_to_delete))
    # move all nested folders and files from
    # sub scene folder (0 or 1) one level up to scene folder 
    for elem in os.listdir(sub_scene_path):
        shutil.move(os.path.join(sub_scene_path, elem), scene_full_path)
    shutil.rmtree(sub_scene_path)
        
    
    # extract from metadata file granule name, 
    # creates sub folder GRANULE/{granule_name}
    # and moves all raster bands and other files inside it
    metadata_file = open(os.path.join(scene_full_path,'MTD_MSIL2A.xml'),'r')
    metadata = metadata_file.read()
    metadata_file.close()
    granule_name = ''
    m = re.search("GRANULE/([^/]*)/", metadata)
    if m:
        granule_name = m.group(1)
            
    granule_path = os.path.join(scene_full_path,'GRANULE')
    os.mkdir(granule_path)
    granule_path = os.path.join(granule_path, granule_name)
    os.mkdir(granule_path)
    
    shutil.move(os.path.join(scene_full_path,'metadata.xml'), os.path.join(granule_path,'MTD_TL.xml'))
    
    granule_path = os.path.join(granule_path, 'IMG_DATA')
    os.mkdir(granule_path)
    
    shutil.move(os.path.join(scene_full_path, 'R10m'), granule_path)
    shutil.move(os.path.join(scene_full_path, 'R20m'), granule_path)
    shutil.move(os.path.join(scene_full_path, 'R60m'), granule_path)
    
        
    # renames band file for different resolutions 
    prod_base = scene[38:44] + '_' + scene[11:26]
    for (dirpath, dirnames, filenames) in os.walk(granule_path):
        for f in filenames:
            if (f.endswith('.jp2')):
                os.rename(os.path.join(dirpath,f),
                    os.path.join(dirpath, prod_base + '_' + f.replace('.jp2','_' + dirpath[-3:] + '.jp2')))

    return true
    
    
    
# sleep until running processes count greater or equal than max_proc    
def wait_for(max_proc, proc_in_work):
    while len(proc_in_work) >= max_proc :
        for proc in proc_in_work:
            if (proc.poll()!=None) : 
                proc_in_work.remove(proc)
                return
        time.sleep(1)
    return



# jp2 to tiff converter (using gdal_translate command line util)
def convert2tiff (scene_full_path, max_proc = 1):
 
    scene = os.path.basename(scene_full_path)
    
    # forms list of rasters to convert (other rasters aren't converted),  
    raster_band_endings = [band + '_10m.jp2' for band in ['B02', 'B03', 'B04', 'B08'] ]
    raster_band_endings += [band + '_20m.jp2' for band in ['B05', 'B06', 'B07', 'B8A', 'B12','B11', 'SCL'] ]
    raster_band_endings += [band + '_60m.jp2' for band in ['B01', 'B09'] ]


    # ToDO other way delete .jp2
                
    # we just loop through all files and
    # converts jp2 -> tif
    for (dirpath, dirnames, filenames) in os.walk(scene_full_path):
        #convert jp2 -> geotiff
        for f in filenames:
            if not f.endswith('.jp2'): continue
            
            for be in raster_band_endings:
                if f.endswith(be): 
                    if args.a and os.path.exists(os.path.join(new_dirpath,f).replace('.jp2','.tif')):
                        break
                    wait_for(max_proc,proc_in_work)
                    command = ('gdal_translate -of GTiff -co COMPRESS=LZW ' + os.path.join(dirpath,f) + ' ' + 
                              os.path.join(new_dirpath,f).replace('.jp2','.tif'))
                    proc_in_work.append(subprocess.Popen(command,shell=True,stdout=subprocess.DEVNULL))
                    break
    wait_for(1,proc_in_work) # wait when all conversions are done
    
    # delete all .jp2 files
    for (dirpath, dirnames, filenames) in os.walk(scene_full_path):
        for f in filenames:
            if f.endswith('.jp2'):
                os.remove(os.path.join(dirpath,f))
    
    return true
    
    
#############################################     
########################## MAIN
#############################################


parser = argparse.ArgumentParser(description='Converts scenes downloaded from AWS to SciHUB file names format')

parser.add_argument('-i', required=True, metavar='input folder', help='Input folder with AWS L2A products')
parser.add_argument('-p', required=False, type=int, metavar='processes num', help='num of parallel processes',default=1)

if (len(sys.argv)==1):
    parser.print_usage()
    exit(0)

args = parser.parse_args()

for scene in os.listdir(args.i):
    if aws2schihub(os.path.join(args.i,scene)):
        if convert2tiff(os.path.join(args.i,scene),args.p):
            print(f'{scene} - DONE')
            continue
    print(f'{scene} - ERROR')


     