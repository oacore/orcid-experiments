class OrcidSearcher:
    def __init__(self, accessToken, writer):
        self.queue = []
        self.accessToken=accessToken
        self.writer=writer
        self.headers =  headers = {"Content-Type":"application/json",
         "Authorization": "Bearer "+self.accessToken}
    def search(self, coreId, doi):

        if (len(self.queue)<12):
            self.queue.append([self.getSearchUrl(doi),doi, coreId])
        else:
            self.queue.append(self.getSearchUrl(doi))
            requestset = (grequests.get(item[0], headers=self.headers) for item in self.queue)
            startTime = time.time()
            responses = grequests.map(requestset)
            i = 0
            for response in responses:
                if not response:
                    print(response)
                    i+=1
                    continue
                data = response.json()
                orcids=[]

                for profile in data['orcid-search-results']['orcid-search-result']:
                    orcids.append(profile['orcid-profile']['orcid-identifier']['uri'])
                if orcids:
                    print ("%d orcids found for %s doi %s"%(len(orcids), self.queue[i][2],self.queue[i][1]))
                    self.saveResults(self.queue[i][2], self.queue[i][1], orcids)
                i+=1
            endTime = time.time()
            if (endTime-startTime <1):
                    print("Ops, Hit the rate limit, sleep")
                    time.sleep(1)
            self.queue=[]

    def getSearchUrl(self, doi):
        return "https://pub.orcid.org/v1.2/search/orcid-bio/?defType=edismax&q=digital-object-ids:%22"+doi+"%22"
    def saveResults(self, coreId, doi, orcids):
        for orcid in orcids:
            row = (coreId, doi, orcid)
            self.writer.writerow(row)
