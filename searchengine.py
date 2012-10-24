import urllib2
from BeautifulSoup import *
from urlparse import urljoin
from pysqlite2 import dbapi2 as sqlite


# Create a list of words to ignore
ignore_words = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])

class crawler:

	#Initialize the crawler with the name of database
	def __init__(self, dbname):
		self.con = sqlite.connect(dbname)		

	def __del__(self):
		self.con.close()

	def dbcommit(self):
		self.con.commit()
	

	# Auxilliary function for getting an entry id and adding
	# it if it's not present
	def get_entry_id(self, table, field, value, createnew=True):
		cur = self.con.execute(
		"select rowid from %s where %s = '%s'" % (table, field, value))
		res = cur.fetchone()
		if res == None:
			cur = self.con.execute(
			"insert into %s (%s) values ('%s')" % (table, field, value))
			return cur.lastrowid
		else:
			return res[0]


	# Index an individual page
	def add_to_index(self, url, soup):
		if self.is_indexed(url): return
		print 'Indexing ' + url

		# Get the individual words
		text = self.get_text_only(soup)
		words = self.separate_words(text)

		# Get the Url id
		urlid = self.get_entry_id('urllist', 'url', url)

		# Link each word to this url
		for i in range(len(words)):
			word = words[i]
			if word in ignore_words: continue
			wordid = self.get_entry_id('wordlist', 'word', word)
			self.con.execute("insert into wordlocation(urlid, wordid, location) \
				values (%d, %d, %d)" % (urlid, wordid, i))

	# Extract the text from an HTML page (no tags)
	def get_text_only(self, soup):
		v = soup.string
		if v == None:
			c = soup.contents
			resulttext = ''
			for t in c:
				subtext = self.get_text_only(t)
				resulttext += subtext + '\n'
			return resulttext
		else:
			return v.strip()

	# Separate the words by any non-whitespace character
	def separate_words(self, text):
		splitter = re.compile('\\W*')
		return [s.lower() for s in splitter.split(text) if s!='']

	# Return true if this url is already indexed
	def is_indexed(self, url):
		u = self.con.execute \
			("select rowid from urllist where url='%s'" % url).fetchone()
		if u != None:
			# Check if it has actually been crawled
			v = self.con.execute(
			'select * from wordlocation where urlid=%d' % u[0]).fetchone()
			if v != None: return True
		return False

	# Add a link between two pages
	def add_link_ref(self, urlFrom, urlTo, linkText):
		words = self.separate_words(linkText)
		fromid = self.get_entry_id('urllist', 'url', urlFrom)
		toid = self.get_entry_id('urllist', 'url', urlTo)
		if fromid == toid: return
		cur = self.con.execute("insert into link(fromid, toid) values (%d, %d)" %
			(fromid, toid))
		linkid = cur.lastrowid
		for word in words:
			if word in ignore_words: continue
			wordid = self.get_entry_id('wordlist', 'word', word)
			self.con.execute("insert into linkwords(linkid, wordid) values (%d, %d)" %
				(linkid, wordid))

	# Starting with a list of pages, do a breadth
	# first search to the given depth, indexing pages
	# as we go
	def crawl(self, pages, depth=2):
		for i in range(depth):
			newpages = set()
			for page in pages:
				try:
					c = urllib2.urlopen(page)
				except:
					print "Could not open %s" % page
					continue
				soup = BeautifulSoup(c.read())
				self.add_to_index(page, soup)

				links = soup('a')
				for link in links:
					if ('href' in dict(link.attrs)):
						url = urljoin(page, link['href'])
						if url.find("'") != -1: continue
						url = url.split('#')[0] #remove location portion
						if url[0:4] == 'http' and not self.is_indexed(url):
							newpages.add(url)
						linkText = self.get_text_only(link)
						self.add_link_ref(page, url, linkText)

				self.dbcommit()

			pages = newpages


	def create_index_tables(self):
		self.con.execute('create table urllist(url)')
		self.con.execute('create table wordlist(word)')
		self.con.execute('create table wordlocation(urlid, wordid, location)')
		self.con.execute('create table link(fromid integer, toid integer)')
		self.con.execute('create table linkwords(wordid, linkid)')
		self.con.execute('create index wordidx on wordlist(word)')
		self.con.execute('create index urlidx on urllist(url)')
		self.con.execute('create index wordurlidx on wordlocation(wordid)')
		self.con.execute('create index urltoidx on link(toid)')
		self.con.execute('create index urlfromidx on link(fromid)')
		self.dbcommit()
