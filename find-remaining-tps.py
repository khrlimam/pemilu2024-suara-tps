#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='Find missing tps code on downloaded dataset.')
parser.add_argument('name', type=str, help='nama file sumber')
parser.add_argument('dst', type=str, help='Lokasi file disimpan')

if __name__ == '__main__':
    args = parser.parse_args()
    df = pd.read_csv(args.name, low_memory=False)
    df2 = pd.read_csv('res/original-tps.csv', low_memory=False)
    remaining = df2[df2['tps'].isin(df.tps) == False]
    remaining.tps.to_csv(args.dst, index=False, header=None)
    print('Berhasil, Sisa TPS disimpan di file %s' % args.dst)