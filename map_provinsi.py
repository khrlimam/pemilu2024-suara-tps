from util import *

provinsi = pemilu.req(pemilu.wilayah(0))
kode_to_provinsi = {item.get('kode'): item.get('nama') for item in provinsi}