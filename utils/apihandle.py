import requests,sys,json

class ApiHandle():

    def __init__(self,token = '',contenttype = 'json'):
        self.s = requests.Session()

        if token !='':
            self.s.headers['Authorization'] = "Bearer " + token

        self.apptype = 1
        if contenttype == 'json':
            self.s.headers["Content-Type"] = "application/json"
        else:
            self.apptype = 2
            self.s.headers["Content-Type"] = "application/x-www-form-urlencoded"

        pass


    def post(self,url = '',data = None):
        '''

        :param url:
        :param data: ×Öµä
        :param contenttype:
        :return:
        '''
        if self.apptype == 1:
            req = self.s.post(url=url, json=data)
        else:
            req = self.s.post(url=url, data=data)

        req_status_code = req.status_code

        if req_status_code == 200:
            apiresult = req.content
            return apiresult
        elif req_status_code == 403:
            print('token invalid')
            returncode = 0
        elif req_status_code == 500:
            print('internal error')
            returncode = 1
        else:
            print(req.content)
            returncode = 2
        sys.exit(returncode)
        pass


    def get(self,url,data = None):
        '''

        :param url:
        :param data:
        :return:
        '''

        res = self.s.get(url, params = data)

        if res.status_code == 200:
            textdata = res.text
            try:
                json_text = json.loads(textdata)
            except Exception as e:
                json_text = textdata
            finally:
                return json_text
        else:
            print(res)
            sys.exit(0)
        pass