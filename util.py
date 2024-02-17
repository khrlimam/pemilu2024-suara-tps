import crawl_tps as pemilu

def add_web_page(df):
    df['url_webpge'] = df.apply(lambda row: pemilu.tps_webpage(row.tps), axis=1)
    
def add_provinsi(df):
    df['kode_provinsi'] = df.apply(lambda row: str(row.tps)[:2], axis=1)
    df['nama_provinsi'] = df.apply(lambda row: kode_to_provinsi[str(row.kode_provinsi)], axis=1)
    
def find_by_tps(dt, tps):
    return dt.loc[dt['tps'] == str(tps)]

def update(current, updated):
    remove_old_row = current[current['tps'].isin(updated['tps']) == False]
    return remove_old_row.append(updated, ignore_index=True)

def percentage(data, paslon):
    return data[paslon].sum()/data.total_suara_paslon.sum()

def print_percent(data, paslon):
    return "%s (%.02f%%)" % (data[paslon].sum(), percentage(data, paslon)*100)
    
provinsi = pemilu.req(pemilu.wilayah(0))
kode_to_provinsi = {item.get('kode'): item.get('nama') for item in provinsi}