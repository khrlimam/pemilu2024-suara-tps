import crawl_tps as pemilu
import pandas as pd

def add_web_page(df):
    df['url_webpge'] = df.apply(lambda row: pemilu.tps_webpage(row.tps), axis=1)
    
def add_kode_provinsi(df):
    df['kode_provinsi'] = df.apply(lambda row: str(row.tps)[:2], axis=1)

def add_nama_provinsi(df, prov):
    df['provinsi'] = df.apply(lambda row: prov[str(row.tps)[:2]], axis=1)

def add_kode_nama_provinsi(df, prov):
    add_kode_provinsi(df)
    add_nama_provinsi(df, prov)

def add_kota(df, kab):
    df['kab'] = df.apply(lambda row: kab[str(row.tps[:4])], axis=1)
    
def find_by_tps(dt, tps):
    return dt.loc[dt['tps'] == str(tps)]

def update(current, updated):
    remove_old_row = current[current['tps'].isin(updated['tps']) == False]
    return pd.concat([remove_old_row, updated], ignore_index=True)

def percentage(data, paslon):
    return data[paslon].sum()/data.total_suara_paslon.sum()

def print_percent(data, paslon):
    return "%s (%.02f%%)" % (data[paslon].sum(), percentage(data, paslon)*100)

def map_kota_tps(provinsi):
    tmp = {}
    for it in provinsi:
        pemilu.req_parallel(pemilu.wilayah(it['kode']), lambda item: [tmp.update({kab['kode']: kab['nama']}) for kab in item])
    return tmp

def filter_kab_indonesia(data):
    return {kode: data[kode] for kode in filter(lambda item: item[:2] != "99", data)}

def req_kab_indonesia():
    return filter_kab_indonesia(map_kota_tps())