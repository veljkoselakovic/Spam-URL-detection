from logging.handlers import DatagramHandler
import pandas as pd

## Load the data
df = pd.read_csv(r'Datasets/url_spam_classification_features.csv')
df.reset_index()

def addUrlLengthsColumn(dataFrame):

    if 'urlLength' in dataFrame.columns:
        return dataFrame

    lens = []

    for index, row in dataFrame.iterrows():
        URL = row['url']
        isSpam = row['is_spam']
        lens.append(len(URL))

    print("Aded URL lengths")
    return dataFrame.assign(urlLength=pd.Series(lens))

def addHostnameLengthsColumn(dataFrame):
    if 'hostnameLength' in dataFrame.columns:
        return dataFrame

    lens = []

    import re

    for index, row in dataFrame.iterrows():
        URL = row['url']
        isSpam = row['is_spam']

        splitURL = re.split('https:|http:', URL)
        hostname = splitURL[1][2:]

        hostname = re.split('www\.', hostname)
        if len(hostname) == 1:
            hostname = hostname[0]
        else:
            hostname = hostname[1]

        hostname = re.split('/', hostname)[0]

        hostname = re.split('\.', hostname)
        hostname = hostname[:-1]
        hostname = '.'.join(hostname)


        
        lens.append(len(hostname))

    print("Aded hostname lengths")

    return dataFrame.assign(hostnameLength=pd.Series(lens))

def addSecureProtocolColumn(dataFrame):

    if 'secureProtocol' in dataFrame.columns:
        return dataFrame

    import re
    secureProtocolRegExp = re.compile(r'https://')

    secureList = []

    for index, row in dataFrame.iterrows():
        URL = row['url']
        isSpam = row['is_spam']

        if secureProtocolRegExp.search(URL):
            secureList.append(1)
        else:
            secureList.append(0)
            # print(URL)

    print("Aded security protocol")
    
    return dataFrame.assign(secureProtocol=pd.Series(secureList))

def addDomainColumns(dataFrame):
    

    import re

    domains = ['com', 'ru', 'org', 'net', 'in', 'ir', 'au', 'uk', 'ua', 'ca', 'edu', 'gov', 'icu', 'xyz', 'cn', 'ml', 'tk',
                'info', 'club', 'xxx', 'porn', 'co', 'us']


    for domain in domains:
        domainList = []
        domain_ = domain

        if domain_ in dataFrame.columns:
            continue

        for index, row in dataFrame.iterrows():
            URL = row['url']
            isSpam = row['is_spam']
            domainRegExp = re.compile(r'\.' + domain + r'[^a-z]')

            if domainRegExp.search(URL):
                domainList.append(1)
            else:
                domainList.append(0)
                # print(URL)

        if sum(domainList) != 0:
            args = {domain : pd.Series(domainList)}
            dataFrame = dataFrame.assign(**args)
            print("Added " + domain)



    return dataFrame

def addDigitsColumn(dataFrame):
    if 'digitNumber' in dataFrame.columns:
        return dataFrame

    digitNumber = []

    import re

    for index, row in dataFrame.iterrows():
        URL = row['url']
        isSpam = row['is_spam']

        numbers = sum(c.isdigit() for c in URL)

        digitNumber.append(numbers)

    print("Aded digit number")

    return dataFrame.assign(digitNumber=pd.Series(digitNumber))

def addKeywordColumns(dataFrame):
    
    words = ['buy','direct','clearance','order','status','shop','dirt','friends',
            'single','meet','babes','score','xxx','income','boss','business','earn','cash',
            'make','money','biz','degree','price','cheap','discount','free','weight','viagra','million',
            'giveaway','claim','offer','win','sign','invest','congratulations','certified','natural','call',
            'apply','instant','now','urgent','casino','bonus','legal','luxury', 'subscribe', 'google', 'facebook', 
            'dropbox', 'twitter']

    for word in words:
        wordList = []
        
        if word in dataFrame.columns:
            continue

        isSpamTriggered = False
        for index, row in dataFrame.iterrows():
            URL = row['url']
            isSpam = row['is_spam']

            if word in URL:
                wordList.append(1)
                if isSpam:
                    isSpamTriggered = True
            else:
                wordList.append(0)
                # print(URL)

        if sum(wordList) > 50 or isSpamTriggered:
            args = {word : pd.Series(wordList)}
            dataFrame = dataFrame.assign(**args)
            print("Added " + word + " Encounters: " + str(sum(wordList)))



    return dataFrame

def addNonsenseColumn(dataFrame):
    if 'nonsense' in dataFrame.columns:
        return dataFrame

    nonsenseList = []

    from nostril import nonsense


    for index, row in dataFrame.iterrows():
        URL = row['url']
        isSpam = row['is_spam']

        urlParted = [x for x in URL.split('/') if len(x)>6];

        nonsenseLength = 0
        for part in urlParted:
            try:
                if nonsense(part):
                    nonsenseLength+=len(part)
            except ValueError:
                pass
        nonsenseList.append(nonsenseLength/len(URL))        

    print("Added nonsense")
    return dataFrame.assign(nonsense=pd.Series(nonsenseList))

def pipeline(dataFrame):
    dataFrame = addUrlLengthsColumn(dataFrame)
    print("-", end='')
    dataFrame = addHostnameLengthsColumn(dataFrame)
    print("-", end='')
    dataFrame = addSecureProtocolColumn(dataFrame)
    print("-", end='')
    dataFrame = addDomainColumns(dataFrame)
    print("-", end='')
    dataFrame = addDigitsColumn(dataFrame)
    print("-", end='')
    dataFrame = addKeywordColumns(dataFrame)
    print("-", end='')
    dataFrame = addNonsenseColumn(dataFrame)
    print("-", end='')
    return dataFrame


#df = pipeline(df)
df = df.drop_duplicates()
df.to_csv('Datasets/url_spam_classification_features.csv', index=False)
print(df)