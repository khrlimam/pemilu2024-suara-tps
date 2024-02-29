#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import requests
from celery import Celery

app = Celery('multi-task-request')


# In[ ]:





# In[3]:


OUTPUT_LOCATION = "dataset"
FILE_NAME = "coba-celery.csv"
dataset_path = os.path.join(OUTPUT_LOCATION, FILE_NAME)
BASE_URL = "https://sirekap-obj-data.kpu.go.id"
TPS_WEB_URL = 'https://pemilu2024.kpu.go.id/pilpres/hitung-suara';
PPWP = f'{BASE_URL}/pemilu/ppwp.json'
WILAYAH_PARTITION = (2,4,6,10,13)
PASLON01_ID = '100025'
PASLON02_ID = '100026'
PASLON03_ID = '100027'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en,en-US;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Origin': 'https://pemilu2024.kpu.go.id',
    'Connection': 'keep-alive',
    'Referer': 'https://pemilu2024.kpu.go.id/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Sec-GPC': '1',
}

def base_request(url, *kode):
    tree = '/'.join(map(str, kode))
    return f"{url}/{tree}.json"

def wilayah(*kode):
    return base_request(f"{BASE_URL}/wilayah/pemilu/ppwp", *kode)

def hhcw(*kode):
    return base_request(f"{BASE_URL}/pemilu/hhcw/ppwp", *kode)

def tps_webpage(tps):
    partitioned = '/'.join(get_tps_url_partition(tps))
    return f"{TPS_WEB_URL}/{partitioned}"

@app.task
def req_parallel(url, fn):
    res = requests.get(url, headers=HEADERS).json()
    fn(res)
    
def req(url):
    return requests.get(url, headers=HEADERS).json()

def get_tps_url_partition(tps):
    return [str(tps)[:part] for part in WILAYAH_PARTITION]

# In[12]:


def create_file():
    try:
        os.makedirs(OUTPUT_LOCATION)
        with open(dataset_path, 'a') as f:
            f.write("tps,paslon01,paslon02,paslon03,total_suara_paslon,suara_sah,suara_paslon-suara_sah,suara_tidak_sah,total_suara,img1,img2,img3\n")
    except:
        print("file %s exists" % dataset_path)

def write(*data):
    with open(dataset_path, 'a') as f:
        f.write(f"{','.join(map(str, data))}\n")

# In[22]:


def safe_get(obj, key, default="-"):
    try:
        #anticiapte None returned
        return obj.get(key, default) or default 
    except:
        return default;


# In[13]:


# @multitasking.task
def get_province(kode=0):
    req_parallel(wilayah(kode), lambda item: loop_province(item))
    
def loop_province(data):
    for prov in data:
        print("Mengambil data suara pada TPS Provinsi %s" % prov.get('nama'))
        get_kabupaten(prov.get("kode"))

# @multitasking.task
def get_kabupaten(prov):
    req_parallel(wilayah(prov), lambda item: loop_kabupaten(prov, item))
    
def loop_kabupaten(prov, data):
    for kab in data:
        get_kecamatan(prov, kab.get("kode"))

# @multitasking.task
def get_kecamatan(prov, kab):
    req_parallel(wilayah(prov, kab), lambda item: loop_kecamatan(prov, kab, item))
    
def loop_kecamatan(prov, kab, data):
    for kec in data:
        get_lurah(prov, kab, kec.get("kode"))

# @multitasking.task
def get_lurah(prov, kab, kec):
    req_parallel(wilayah(prov, kab, kec), lambda item: loop_lurah(prov, kab, kec, item))

def loop_lurah(prov, kab, kec, data):
    for lurah in data:
        get_tps(prov, kab, kec, lurah.get("kode"))
    

# @multitasking.task
def get_tps(prov, kab, kec, lurah):
    req_parallel(wilayah(prov, kab, kec, lurah), lambda item: loop_tps(prov, kab, kec, lurah, item))
    
def loop_tps(prov, kab, kec, lurah, data):
    for tps in data:
        suara_tps(prov, kab, kec, lurah, tps)

@app.task
def suara_tps(prov, kab, kec, lurah, tps):
    suara = req(hhcw(prov, kab, kec, lurah, tps.get("kode")))
    process_suara(tps, suara)

def process_suara(tps, suara):
    tps_counter =  tps_counter+1
    print("tps process %s" % tps_counter)
    kode_tps = tps.get("kode")
    administrasi = safe_get(suara, 'administrasi', {})
    img1, img2, img3 = tuple(safe_get(suara, 'images', ('-','-','-')))
    chart = safe_get(suara, 'chart', {})
    suara1 = safe_get(chart, PASLON01_ID, 0)
    suara2 = safe_get(chart, PASLON02_ID, 0)
    suara3 = safe_get(chart, PASLON03_ID, 0)
    suara_sah = safe_get(administrasi, "suara_sah", 0)
    suara_tidak_sah = safe_get(administrasi, "suara_tidak_sah", 0)
    total_suara = safe_get(administrasi, 'suara_total', 0)
    total_suara_paslon = int(suara1) + int(suara2) + int(suara3)
    suara_paslon_min_suara_sah = total_suara_paslon-suara_sah
    write(kode_tps,suara1,suara2,suara3,total_suara_paslon,suara_sah,suara_paslon_min_suara_sah,suara_tidak_sah,total_suara,img1,img2,img3)


# In[24]:

tps_counter = 0
if __name__ == "__main__":
    create_file()
    tps_counter = 0
    get_province()


# In[ ]:




