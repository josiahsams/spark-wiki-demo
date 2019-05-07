from __future__ import print_function
from bs4 import BeautifulSoup
import requests
import urllib
import os
import sys
import re

basepath = "https://dumps.wikimedia.org/other/pageviews/2018/"
directory = "./data"
dayReq = "20180518"


regex = re.compile('.*'+dayReq+'.*')
r  = requests.get(basepath)

data = r.text
soup = BeautifulSoup(data, 'lxml')

if not os.path.exists(directory):
    os.makedirs(directory)

for link in soup.find_all('a'):
    subpath = link.get('href')
    if subpath == "../":
        continue
    s = requests.get(basepath + subpath)
    data2 = s.text
    soup2 = BeautifulSoup(data2, 'lxml')
    for link2 in soup2.find_all('a'):
        filename = link2.get('href')
        if filename == "../" :
           continue
        if not re.match(regex, filename):
           continue

        fullpath = basepath + subpath + filename
        print("Downloading " +  fullpath + " to " + directory + '/'+filename + " .. " , end="")
        sys.stdout.flush()
        urllib.urlretrieve(fullpath, filename=directory + '/'+filename)
        print("Done")
       # content = response.read()
       # with open(directory + '/'+filename, "wb") as file:
       #     file.write(content)
