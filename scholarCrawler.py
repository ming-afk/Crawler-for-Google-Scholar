import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
import re
from bs4 import BeautifulSoup
import urllib.request
import sys

###Defining global variables

pages = int(input ("Enter number of pages to crawl: "))

conn = sqlite3.connect('scholars.sqlite')
cur = conn.cursor()

cur.executescript('''

CREATE TABLE IF NOT EXISTS Profileurl
(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, url TEXT UNIQUE);

CREATE TABLE IF NOT EXISTS Scholars
(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, name TEXT UNIQUE, citation INTEGER, profile_id INTEGER UNIQUE)
''')
conn.commit()

cur.execute("select * from Scholars")
rowNum = len(cur.fetchall())

limit = pages + rowNum
user_agent = 'Mozilla/5.0'
headers = {'User-Agent': user_agent }

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

#######this function checks if a co-author list exits on any google scholar page
#######input: url list
#######output: boolean. if True, list of newurls directing to each scholar's main profile page is also returned
def checkCoauthor(urllist):
    newurl = []
    for url in urllist:
        request = urllib.request.Request( url=url, headers=headers )
        req = urllib.request.urlopen(request)
        soup = BeautifulSoup(req.read(), 'html.parser')
        tags = soup.find_all('span')

        for tag in tags:
            tag = str(tag)
            rawurl = re.findall('<a href="(.*?)"',tag.strip("\u202a\u202c"))
            if rawurl != []:
                string = ""
                ###now retrieve normal url from AMP url
                rawurl = 'https://scholar.google.ca' + string.join(rawurl[0].split("amp;")).strip("'") #we tidy up the url by removing '&amp;'
                newurl.append(rawurl)
          
    if newurl == []:
        return False,None
    return True,newurl


def extendurl(urllist):
#The start url: 
#during url collection, output of extendurl is consistently fed back to itself
#after url collection, starturl of each crawl contains the single lastest url that is added to the database
#but before tables are created, starturl is provided by the user.
    newurl = [] 
    flag, newurl = checkCoauthor(urllist)
    if flag:
        return newurl
        
    else: #if no co-author list
        print("no co-authors found, trying to retrieve field-relative authors")
        print("")
        
        fieldpages = []
        for url in urllist: #search for field-relative author for each url
            
            request = urllib.request.Request(url=url, headers=headers)
            req = urllib.request.urlopen(request)
            soup = BeautifulSoup(req.read(), 'html.parser')
            tags = soup.find_all ('a')
            #print("TAG LENGTH",len(tags))
            ###generating fieldpages list, which contains all field urls
            for tag in tags:
                tag = str(tag)
                ###all profiles contain an url that has "mauthors" to direct to 
                ###a page containing all researcher profiles under one lable
                fieldurl = re.findall('<a .+href="(.*mauthors.*?)"',tag.strip("\u202a\u202c"))
                print(fieldurl)
                if fieldurl!=[]:
                    string = ""
                    fieldurl = 'https://scholar.google.ca' + string.join(fieldurl[0].split("amp;")).strip("'")
                    fieldpages.append(fieldurl)
        
            #print("FIELDPAGES GENERATED,LENGTH IS ",len(fieldpages))
        
            newurl=[]
            #For each field url, go to the page and retrieve all scholar urls
            for fieldurl in fieldpages:
                request = urllib.request.Request(url = fieldurl, headers = headers)
                req = urllib.request.urlopen(request)
                soup= BeautifulSoup(req.read(), 'html.parser')
                tags = soup.find_all('a')
                for tag in tags:
                    tag = str(tag)
                    ###retrieve all url that has "user" in it
                    rawurl = re.findall('<a href="(.*user.*?)"',tag.strip("\u202a\u202c"))
                    if rawurl!=[]: #if there is people under one field label
                        string = ""
                        rawurl = 'https://scholar.google.ca' + string.join(rawurl[0].split("amp;")).strip("'")
                        newurl.append(rawurl)
            #print("NEWURL GENERATED,LENGTH IS",len(newurl))
        return newurl


################### scholarCrawler ########################

def scholarCrawler(urllist):
    #print("IN CRAWLER,LIST LENGTH IS")
    for url in urllist:
        ###a typical url: https://scholar.google.ca/citations?hl=en&user=oB0_OKoAAAJ
        ###By chance, the urls we retrieve can end with either:
        ###hl=en&user=...  OR
        ###user=...&hl=en
        ###we want to extract the second format
        
        if url.find("user")>url.find("hl=en"):
            p1,p2 = url.split("?")
            l1,l2=(p2).split("&")
            url = "?".join([p1,"&".join([l2,l1])]) #flip the order
            
        cur.execute("select * from Scholars")
        rowNum = len(cur.fetchall())
        if rowNum<limit: #if we have not read enough
            request = urllib.request.Request( url=url, headers=headers )
            req = urllib.request.urlopen(request)
            soup = BeautifulSoup(req.read(), 'html.parser')
            tags = soup('head')

            for tag in tags:
                ###one typical name format: <!doctype html><html><head><title>‪***‬ - ‪Google Scholar‬</title>
                name = re.findall('<head><title>(.*) - ‪Google Scholar',str(tag))[0].strip("\u202a\u202c")
                citation = re.findall('<meta content=.*Cited by ([,0-9]*)',str(tag))

                citation = citation[0].split(',')
                cite = 0
                for i in citation:
                    cite = cite*1000+int(i)

                try:
                    cur.execute('''INSERT INTO Profileurl (url) VALUES(?)''', (url,))
                    conn.commit()
                    cur.execute('''SELECT id FROM Profileurl WHERE url = ?''', (url,))
                    profile_id = cur.fetchone()[0]

                    cur.execute('''INSERT INTO Scholars(name, citation, profile_id) VALUES(?,?,?)''',
                                (name, cite, profile_id))
                    conn.commit()
                except: #except unique constraint violated
                    pass
        else: return
    return


while True:
    #during url collection, output of extendurl is consistently fed back to itself
    #after url collection, starturl of each crawl contains the single lastest url that is added to the database
    #but before tables are created, starturl is provided by the user.

    ###if there are already entries in database, give the lastest url:
    if rowNum!=0:
        print("Crawling continues from lastest link retrieval")
        print("")
    ###selecting newest url retrieved
        cur.execute(''' SELECT url FROM Profileurl WHERE id in (SELECT max(id) FROM Profileurl) ''')
        url = cur.fetchone()[0]

    ###if there is not, feed the user input url:
    else:
        url = input("Enter the url you want to start with:")
    url = [url]

    while True:
        cur.execute("select * from Scholars")
        rowNum = len(cur.fetchall())
        print("Already acquired information from {data} websites, {number} pages left"
              .format(data = rowNum,number = limit-rowNum))
        if rowNum>=limit:
            print("Enough scholars retrieved")
            break;
        scholarCrawler(url)
        url = extendurl(url)
        
    pages = input("Crawling finished, please enter number of pages or press Enter to quit: ")
    
    if pages == "":
        print("Program exited")
        break
    else:
        limit = int(pages) + rowNum
        
# cur.execute(''' SELECT * FROM Scholars ''')
# for i in cur:
#     print("data is: ",i)
