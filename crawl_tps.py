#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import requests
import multitasking


# In[ ]:





# In[3]:


OUTPUT_LOCATION = "dataset"
FILE_NAME = "suara-tps.csv"
dataset_path = os.path.join(OUTPUT_LOCATION, FILE_NAME)
BASE_URL = "https://sirekap-obj-data.kpu.go.id"
PPWP = f'{BASE_URL}/pemilu/ppwp.json'

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

@multitasking.task
def req_parallel(url, fn):
    res = requests.get(url, headers=HEADERS).json()
    fn(res)
    
def req(url):
    return requests.get(url, headers=HEADERS).json()

# In[12]:


def create_file():
    try:
        os.makedirs(OUTPUT_LOCATION)
        with open(dataset_path, 'a') as f:
            f.write("tps,paslon01,paslon02,paslon03,suara_sah,suara_tidak_sah,total_suara,total_suara_paslon,img1,img2,img3\n")
    except:
        print("file %s exists" % dataset_path)

def write(*data):
    print(f"writing: {data[:-3]}")
    with open(dataset_path, 'a') as f:
        f.write(f"{','.join(map(str, data))}\n")

# In[22]:


def safe_get(obj, key, default="-"):
    try:
        return obj.get(key, default)
    except:
        return default;


# In[13]:


@multitasking.task
def get_province(kode=0):
    req_parallel(wilayah(kode), lambda item: loop_province(item))
    
def loop_province(data):
    for prov in data:
        get_kabupaten(prov.get("kode"))

@multitasking.task
def get_kabupaten(prov):
    req_parallel(wilayah(prov), lambda item: loop_kabupaten(prov, item))
    
def loop_kabupaten(prov, data):
    for kab in data:
        get_kecamatan(prov, kab.get("kode"))

@multitasking.task
def get_kecamatan(prov, kab):
    req_parallel(wilayah(prov, kab), lambda item: loop_kecamatan(prov, kab, item))
    
def loop_kecamatan(prov, kab, data):
    for kec in data:
        get_lurah(prov, kab, kec.get("kode"))

@multitasking.task
def get_lurah(prov, kab, kec):
    req_parallel(wilayah(prov, kab, kec), lambda item: loop_lurah(prov, kab, kec, item))

def loop_lurah(prov, kab, kec, data):
    for lurah in data:
        get_tps(prov, kab, kec, lurah.get("kode"))
    

@multitasking.task
def get_tps(prov, kab, kec, lurah):
    req_parallel(wilayah(prov, kab, kec, lurah), lambda item: loop_tps(prov, kab, kec, lurah, item))
    
def loop_tps(prov, kab, kec, lurah, data):
    for tps in data:
        suara_tps(prov, kab, kec, lurah, tps)

@multitasking.task
def suara_tps(prov, kab, kec, lurah, tps):
    suara = req(hhcw(prov, kab, kec, lurah, tps.get("kode")))
    process_suara(tps, suara)
    
def process_suara(tps, suara):
    kode_tps = tps.get("kode")
    nama_tps = tps.get("nama")
    administrasi = safe_get(suara, 'administrasi', {})
    img1, img2, img3 = tuple(safe_get(suara, 'images', ('-','-','-')))
    chart = suara.get('chart', {})
    suara1 = safe_get(chart, '100025', 0)
    suara2 = safe_get(chart, "100026", 0)
    suara3 = safe_get(chart, "100027", 0)
    suara_sah = safe_get(administrasi, "suara_sah", 0)
    suara_tidak_sah = safe_get(administrasi, "suara_tidak_sah", 0)
    total_suara = safe_get(administrasi, 'suara_total', 0)
    total_suara_paslon = int(suara1) + int(suara2) + int(suara3)
    write(kode_tps,suara1,suara2,suara3,suara_sah,suara_tidak_sah,total_suara,total_suara_paslon,img1,img2,img3)


# In[24]:

if __name__ == "__main__":
    create_file()
    get_province()


# In[ ]:




