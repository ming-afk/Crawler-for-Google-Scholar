# Crawler-for-Google-Scholar

**Python web crawler to crawl scholar citations and generates SQL database based on co-author and research field labels.**

**The crawler uses BeautifulSoup library: https://www.crummy.com/software/BeautifulSoup/bs4/doc/ and urllib library in python.**

If Co-author information is not available, the crawler follows the author's research field and pulls information from the author's collegues.

On consecutive runs, the crawler searches for the lastest updated url and picks up from there.

Have fun with it!
