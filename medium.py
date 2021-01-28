class Medium():
    
    '''This class scrapes and store Medium articles'''
    def __init__(self, tag, start_year):
        '''Please insert the tag you want to scrape and the year you want the scraper starts.
        
        When this class is initialized the first extraction starts.
        The first extraction is enabled by the method `self.extract_config()`. Namely it starts extracting for each date the url of the articles published in that specific date.
        
        There three objects in this class:
        1. `self.confing`: a list that contains all the dates where there is at least one article.
        2. `self.buffer`: a list that contains the html already stored locally.
        3. `self.data`: this is a DataFrame that contains the articles extracted.
        '''
        
        
        if not os.path.exists('./data/buffer'):
            os.mkdir('./data/buffer')
            os.mkdir('./data/buffer/first_extraction/')
            os.mkdir('./data/buffer/second_extraction/')
        
        self.tag = tag
        self.start_year = start_year
        
        self.url = 'https://medium.com/tag/%s'%(tag)
        self.config = self.extract_config()
        self.buffer = self.buffering(iteration = 'first_extraction')
        
        self.data = None
        
    
        
    def extract_config(self):
        '''This method extracts or creates the config.txt file for the first extraction) 
        
        The config as the role of avoiding the requests for dates where there are no published articles.
        In this config there are the links of each day/month/year where at least one article is published.'''
        
        
        if os.path.exists('./data/buffer/first_extraction/config.txt'):
            
            return open("./data/buffer/first_extraction/config.txt", "r").readlines()
            
        else:
            print('Checking archive...')
            config = []
            
            for year in range(int(self.start_year),2021):
                
                print('Checking year %s...'%(str(year)))
                response = requests.get(self.url + '/archive/' + str(year))
                soup = BeautifulSoup(response.content,'lxml')
                
                months_tag = soup.find_all('div', attrs = {'class': 'timebucket u-inlineBlock u-width80'})
                
                
                if months_tag == []:
                    config.append(soup.find('link')['href'])
                for div in months_tag:
    
                    getter = div.find('a')

                    if getter is not None:


                        response = requests.get(getter['href'])
                        soup = BeautifulSoup(response.content,'lxml')

                        days_tag = soup.find_all('div', attrs = {'class':'timebucket u-inlineBlock u-width35'})

                        month = soup.find('link')['href']

                        if days_tag == []:
                            config.append(month)
                            continue

            #            print('Checking %s %s days...'%(month, year))
                        for div in days_tag:

                            getter = div.find('a')

                            if getter is not None:

                                config.append(getter['href'])
            
            with open('./data/buffer/config.txt', 'w') as f:
                for item in config:
                    f.write("%s\n" % item)
                    
        return [f.split('archive/')[1].replace('\n','').replace('/','-') for f in config]
            
            

    def buffering(self, iteration):
        '''This method iterate over the folders of the the two extractions and fills the `self.buffer` object. This method avoid the repetition of requests of urls checking what are the files already stored.'''
        
        if iteration == 'first_extraction':
            buffer = [f.split('.')[0] for f in os.listdir('./data/buffer/first_extraction') if (f != 'config.txt') & ('check' not in f)]
            return list(set(self.config) - set(buffer))
        if iteration == 'second_extraction':
            return [f.split('.')[0] for f in os.listdir('./data/buffer/second_extraction') if (f != 'config.txt') & ('check' not in f)]
            
        
    
    def extract_articles(self):
        print('Extracting missing articles')
        for page in tqdm(self.buffer):
            
            response = requests.get(self.url + '/archive/' + page.replace('-','/'))
            soup = BeautifulSoup(response.content,'lxml')
            
            href = soup.find('link')['href']
    
            results = []
    
            for title in soup.find_all('h3'):
                results.append([href,title.get_text(),title.find_previous('a')['href'].split('?')[0]])

            pd.DataFrame(results, columns = ['date','title','url']).to_csv('./data/buffer/first_extraction/%s.csv'%(page), index = False)
        
        
    def extract_data(self):
        
        '''This method fills the `self.data` DataFrame.
        
        It works as follow:
            1. it iterates over the folder `./data/buffer/first_extraction`. In this folder there are .xlsx files for each date. Namely, in each .xlsx there are the links (url) of the articles published in the specific day;
            2. each .xlsx are read (imported in pandas) and each article inside the DataFrame are added to the `self.data` dataframe. In this phase only the url and id of the articles are present.
        '''
        
        if os.path.exists('./data/medium_%s.xlsx'%(self.tag)):
            self.data = pd.read_excel('./data/medium_%s.xlsx'%(self.tag))
            
        else:
            data = pd.concat([pd.read_csv('./data/buffer/first_extraction/' + f) for f in os.listdir('./data/buffer/first_extraction') if (f != 'config.txt') & ('check' not in f)])

        
            data['pubDate'] = data['date'].apply(lambda x: pd.to_datetime(x.split('archive/')[-1].replace('/','-')))
            
            self.data = []
            
            for pubDate in data.pubDate.unique():
                date = data[data['pubDate'] == pubDate].reset_index()
    
                date['pubId'] = date.apply(lambda x: 
                           'Y' + str(x['pubDate'].year) + 
                           'M' + str(x['pubDate'].month) + 
                           'D' + str(x['pubDate'].day) + 
                           'N' + "%04d" % (x.name + 1,) , axis = 1)
        
                self.data.append(date)
            
            self.data = pd.concat(self.data).drop('index',axis = 1)
            self.data = self.data[self.data['url'].str.contains('medium')].dropna(subset=['url']).reset_index()
            
            self.data.to_excel('./data/medium_%s.xlsx'%(self.tag))
        
        
        
        print('Data extracted')
        return self.data
    
    def get_tags(self, soup):
        '''This method extracts the tags of the articles.
        It takes as input a soup and returns a list.'''

        
        tags = []
        for tag in soup.find_all('a', attrs = {'href':re.compile("(tag)"),'class':['au', 'b', 'ea', 'om', 'ax', 'on', 'oo', 'hp', 's', 'op']}):
            tags.append(tag.get_text())
        return '; '.join(tags)

    def get_text(self, soup):
        '''This method extracts the full text of the article.
        It takes as input a soup and returns a string.'''


        text = ''

        for div in soup.find_all('div', attrs = {'class':['ae', 'af', 'ag', 'ah', 'ai', 'eg', 'ak', 't']}):
            for c in div.contents:
                if c.name == 'p':
                    text += '\n\n'
                    text += c.get_text()
                if c.name == 'ol':
                    text += '\n\n\\begin_list'
                    for l in c.contents:
                        text += '\n'
                        text += l.get_text()
                    text += '\n\\end_list'
                if (c.name == 'h1') | (c.name == 'h2') | (c.name == 'h3'):
                    text += '\n\n\\begin_title\n'
                    text += c.get_text()
                    text += '\n\\end_title'

        return text
    
    def get_links(self, soup):
        '''This method extracts the hyperlinks contained in the full text of the article.
        It takes as input a soup and returns a list.'''
        
        links = []
        for div in soup.find_all('div', attrs = {'class':['ae', 'af', 'ag', 'ah', 'ai', 'eg', 'ak', 't']}):
            for c in div.contents:
                if c.name == 'p':
                    link = c.find('a')
                    if link is not None:
                        links.append(link['href'])
        return links
    
    def dump_articles(self):
        '''This method starts the second phase (or second iteration) of the Scraping Process.
        It takes as input the `self.data` DataFrame. It iterates over this DataFrame doing the following thigs:
        1. Goes to the url of the articles
        2. Stores the .html in the buffer folder `./data/buffer/second_extraction`
        
        The name of the file stored is the ID of the article.'''
        
        if not os.path.exists('./data/buffer/second_extraction/'):
            
            os.mkdir('./data/buffer/second_extraction/')
                    
        buffer = self.buffering(iteration = 'second_extraction')
        
        tmp = self.data[['pubId','url']]
        tmp = tmp[~tmp['pubId'].isin(buffer)]
        
        for _,row in tqdm(tmp.iterrows(),total = len(tmp)):
#            print(row['url'])
            response = requests.get(row['url'])
            soup = BeautifulSoup(response.content,'lxml')
            
            with open("./data/buffer/second_extraction/%s.html"%(row['pubId']), "w") as file:
                file.write(str(soup))

        
    
    def extract_single_articles(self):
        '''This method extracts the content of each article. 
        
        The steps are the followings:
            1. It first dump the articles with the method `dump_articles`, namely stores the html page of the article in a folder.
            2. For each html file inside the `./data/buffer/second_extraction` it extracts the content, the tags, and hyperlinks.
            
        The extractions go in the `self.data` DataFrame.
        '''
        
        print('Dumping articles...')
        self.dump_articles()
        
        if 'content' in self.data.columns:
            return None
        
        self.data['content'] = np.nan
        self.data['tags'] = np.nan
        self.data['links'] = np.nan
        
        print('Reading html...')
        
        
        for _,row in tqdm(self.data.iterrows(), total = len(self.data)):
            
            soup = BeautifulSoup(open("./data/buffer/second_extraction/%s.html"%(row['pubId'])), "html.parser")
            self.data.loc[_,'content'] = self.get_text(soup)
            self.data.loc[_,'tags'] = self.get_tags(soup)
            self.data.loc[_,'links'] = '; '.join(self.get_links(soup))