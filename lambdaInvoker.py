import boto3
import json
import concurrent.futures
import random
import pymysql
import schedule
import time
lambda_client = boto3.client('lambda')

# endpoint = # rds endpoint
# username = # username
# password = # password
# database_name = # db name
# port = # port


class Invoker:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def main(self):
        try:
            self.connection = pymysql.connect(host=endpoint, user=username, passwd=password, db=database_name, port=port)
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            query = 'select asinId as asin, keyword from keyword_asin limit 50'
            self.cursor.execute(query)
            chunkSize = 20

            while True:
                chunk = self.cursor.fetchmany(chunkSize)
                if not chunk:
                    break
                self.handle_request(chunk)

            # records = self.cursor.fetchall()
            # print(records)

            # self.handle_request(records)

            print('cron job completed')

        except Exception as e:
            print(e)

        finally:
            self.cursor.close()
            self.connection.close()

    def handle_request(self, records):
        payloadSize = 2
        data = []
        for i in range(0, len(records), payloadSize):
            body = {
                "maxPages": "3",
                "pincode": "4328018",
                "input": records[i: i+payloadSize]
            }
            data.append(body)

        values = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.invoke_lambda_function, payload) for payload in data]
            while futures:
                completed, _ = concurrent.futures.wait(futures, timeout=0)
                for future in completed:
                    result = future.result()
                    # print(result)
                    values.append(self.fetchScrapeData(result))
                    futures.remove(future)
        if values:
            storeQuery = "INSERT INTO raw_ranks (asinId, keyword, product_name, image_url, `rank`) VALUES (%s, %s, %s, %s, %s)"
            self.cursor.executemany(storeQuery, values)
            self.connection.commit()
            for i in values:
                print(i)
            print('data inserted successfully')
        else:
            print('no data scraped')
        

    def fetchScrapeData(self, result):
        status = result.get('statusCode', 0)
        if status == 200:
            data = json.loads(result.get('message'))['data']
            for d in data:
                if len(d):
                    return (d.get('asin'), d.get('keyword'), d.get('productName'), d.get('imageUrl'), d.get('rank'))
                


    def invoke_lambda_function(self, payload):
        randomWorker = random.randint(1,5)
        maxRetries = 2
        retry = 0
        print(f'invoked worker{randomWorker}')
        try:
            # cases handled if product is found and product is not found.
            while True:
                functionName = f'arn:aws:lambda:eu-north-1:653897387879:function:worker{randomWorker}'
                response = lambda_client.invoke(
                    FunctionName = functionName,
                    InvocationType= 'RequestResponse',
                    Payload =  json.dumps(payload)
                )
                resPayload = response['Payload'].read()
                res = json.loads(resPayload) 

                if(res['statusCode'] and res['statusCode'] == 200):
                    return res
                
                if retry > maxRetries:
                    break

                retry = retry + 1
                print('retrying',f'worker{randomWorker}')

            print('some err 500')
            return {}
        
            # cases handled after applying retries may be status code 500

        except Exception as e:
            # other exception
            print('Lambda service exception', e)
            return {}


if __name__ == "__main__":
    app = Invoker()
    # runs for every 5 minutes
    schedule.every(5).minutes.do(app.main)
    while True:
        schedule.run_pending()
        time.sleep(1)
    # app.main()
