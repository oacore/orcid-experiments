import grequests
import csv
import time
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth



class OrcidSearcher:
    def __init__(self, accessToken, writer):
        self.queue = []
        self.writer = writer

        self.accessToken = self.getToken()
        self.headers = headers = {"Content-Type": "application/json",
                                  "Authorization": "Bearer " + self.accessToken}




    def getToken(self):
        client_id = "APP-7JEN2756AHK9GJ2F"
        client_secret = "7f0dbea9-1334-4013-b092-d3d65a194836"
        scope = ['/read-public']
        auth = HTTPBasicAuth(client_id, client_secret)
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client, scope=scope)
        return oauth.fetch_token(token_url='https://orcid.org/oauth/token', auth=auth, scope=scope)["access_token"]

    def search(self, coreId, doi):

        if (len(self.queue) < 12):
            self.queue.append([self.getSearchUrl(doi), doi, coreId])
        else:
            self.queue.append(self.getSearchUrl(doi))
            requestset = (grequests.get(item[0], headers=self.headers) for item in self.queue)
            startTime = time.time()
            responses = grequests.map(requestset)
            i = 0
            for response in responses:

                if not response:
                    print(response)
                    if response is not None and response.status_code >= 400:
                        self.accessToken = self.getToken()
                        self.headers = {"Content-Type": "application/json",
                                                  "Authorization": "Bearer " + self.accessToken}
                        print("Refreshing token...")
                        continue
                    i += 1
                    continue
                if response.status_code >= 400:
                    exit()
                data = response.json()
                orcids = []
                print()
                if len(data['result'])>0:
                    print (data['result'])
                    for profile in data['result']:
                        orcids.append(profile['orcid-identifier']['uri'])
                    if orcids:
                        print("%d orcids found for %s doi %s" % (len(orcids), self.queue[i][2], self.queue[i][1]))
                        self.saveResults(self.queue[i][2], self.queue[i][1], orcids)
                    i += 1
            endTime = time.time()
            if (endTime - startTime < 1):
                print("Ops, Hit the rate limit, sleep")
                time.sleep(1)
            self.queue = []

    def getSearchUrl(self, doi):
        return "https://pub.orcid.org/v2.1/search/?q=doi-self:%22" + doi + "%22"

    def saveResults(self, coreId, doi, orcids):
        for orcid in orcids:
            row = (coreId, doi, orcid)
            self.writer.writerow(row)



if __name__ == '__main__':
    startfrom = 110436
    counter = 0

    with open('articles-88.tsv', 'r') as csvfiletoread:
        spamreader = csv.reader(csvfiletoread, delimiter='\t', quotechar='|')
        with open('results/88-orcid-full-8.tsv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            searcher = OrcidSearcher('aa986fc3-621f-4822-8346-2e0ee20108ef', writer)
            for row in spamreader:
                counter += 1
                if counter >=startfrom:
                    if len(row) > 3:
                        if row[3]:
                            searcher.search(row[0], row[3])
                            print("Completed: " + str(counter))
