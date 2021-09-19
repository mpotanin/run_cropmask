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
import errno
import json
from osgeo import gdal

import subprocess
import multiprocessing
import multiprocessing, concurrent.futures
from concurrent.futures import ProcessPoolExecutor


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
    
        
    # renames band files for different resolutions 
    prod_base = scene[38:44] + '_' + scene[11:26]
    for (dirpath, dirnames, filenames) in os.walk(granule_path):
        for f in filenames:
            if (f.endswith('.jp2')):
                os.rename(os.path.join(dirpath,f),
                    os.path.join(dirpath, prod_base + '_' + f.replace('.jp2','_' + dirpath[-3:] + '.jp2')))

    return True
    
    
    
# jp2 to tiff converter (using gdal_translate command line util)
def convert2tiff (scene_full_path):

    #print(scene_full_path)
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
                    command = f'gdal_translate -of GTiff -co COMPRESS=LZW  {os.path.join(dirpath,f)} {os.path.join(dirpath,f).replace(".jp2",".tif")}'
                    p = subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL)
                    p.wait()
                    break
 
    # delete all .jp2 files
    for (dirpath, dirnames, filenames) in os.walk(scene_full_path):
        for f in filenames:
            if f.endswith('.jp2'):
                os.unlink(os.path.join(dirpath,f))
                #os.remove(os.path.join(dirpath,f)) if os.path.exists(os.path.join(dirpath,f)) else print(os.path.join(dirpath,f))
                
    
    return True


def try_read_band_as_array (file_band):
    gdal_ds = gdal.Open(file_band)
    gdal_band = gdal_ds.GetRasterBand(1)
    band_array = gdal_band.ReadAsArray()
    gdal_ds = None
    gdal_band = None
    
    return band_array
    

# verifies if a scene was correctly converted and stored on disk 
# (this is needed if storage isn't reliably)

def verify_scene (scene_path):
    #print(scene_path)
    if not os.path.exists(os.path.join(scene_path,'MTD_MSIL2A.xml')):
        return False
    
    granule_path = os.path.join(scene_path,'GRANULE')
    
    if not os.path.exists(granule_path):
        return False
        
    if len(os.listdir(granule_path)) != 1 :
        return False
        
    
    granule_path = os.path.join(granule_path, os.listdir(granule_path)[0])
    
    if not os.path.exists(os.path.join(granule_path,'MTD_TL.xml')) :
        return False
    
   
    res_list = ['10m','20m','60m']
    check_list = {'10m':['B02','B03','B04','B08'],
                  '20m':['B8A','SCL','B05','B06','B07','B11','B12'],
                  '60m':['B01','B09']}
    
    for res in res_list:
        base_path = os.path.join(granule_path,'IMG_DATA/R' + res)
        for el in check_list[res] :
            found_and_correct = False
            for f in os.listdir(base_path):
                if f.find(el + '_' + res) != -1:
                    (srs,geotransform) = raster_proc.extract_georeference(os.path.join(base_path,f))
                    if (srs is None) or (geotransform is None) :
                        srs = None
                        geotransform = None
                        break
                    #print(os.path.join(base_path,f))
                    if try_read_band_as_array(os.path.join(base_path,f)) is not None :
                        found_and_correct = True
                    break
                    
            if not found_and_correct: return False    
    return True



def convert_single_scene (scene_path, verify = False, max_attempt = 2):
    
    
    if not verify: # simple case - just convert
        if aws2schihub(scene_path):
            if convert2tiff(scene_path): 
                print(f'{os.path.basename(scene_path)}...DONE')
                return True
    else: # complecated case - assume there is not unreliable storage, we try several times to perform conversion
        scene_path_temp = scene_path + '_TEMP'
        shutil.copytree(scene_path,scene_path_temp)
        for i in range(max_attempt):
            try:
                aws2schihub(scene_path_temp)
                convert2tiff(scene_path_temp)
                if verify_scene(scene_path_temp):
                    shutil.rmtree(scene_path)
                    os.rename(scene_path_temp,scene_path)
                    print(f'{os.path.basename(scene_path)}...DONE')
                    return True
                else:
                    raise Exception
            except:
                if os.path.exists(scene_path_temp): 
                    shutil.rmtree(scene_path_temp)
        
    
    print(f'{os.path.basename(scene_path)}...ERROR')    
    return False
 

    
#############################################     
########################## MAIN
#############################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Converts scenes downloaded from AWS to SciHUB file names format')
    
    parser.add_argument('-i', required=True, metavar='input folder', help='Input folder with AWS L2A products')
    parser.add_argument('-vc', required=False, action='store_true', help= "Verify scene files after conversion (needed if storage isn't reliable)")
    parser.add_argument('-p', required=False, metavar='processes num', help='num of parallel processes')

    
    if (len(sys.argv)==1):
        parser.print_usage()
        exit(0)
    
    args = parser.parse_args()
    

    CPU_COUNT = multiprocessing.cpu_count()

    with ProcessPoolExecutor(max_workers = 1 if args.p is None else eval(args.p)) as executor:
        scene_task_list = list()
        for scene in os.listdir(args.i):
            if not scene.startswith('S2') or not os.path.isdir(os.path.join(args.i,scene)): continue
            scene_task_list.append(executor.submit(convert_single_scene, os.path.join(args.i,scene), args.vc))
        concurrent.futures.wait(scene_task_list)
    
    

     