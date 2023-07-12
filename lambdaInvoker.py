import boto3
import json
import concurrent.futures
import random
import pymysql
import schedule
import time
from datetime import date
lambda_client = boto3.client('lambda')

endpoint = ''
username = ''
password = ''
database_name = ''
port= 3306


class Invoker:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def main(self):
        start = time.time()
        try:
            
            self.connection = pymysql.connect(host=endpoint, user=username, passwd=password, db=database_name, port=port)
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            query = 'select asinId as asin, keyword from keyword_asin'
            self.cursor.execute(query)
            chunkSize = 10

            while True:
                chunk = self.cursor.fetchmany(chunkSize)
                if not chunk:
                    break
                self.handle_request(chunk)
            print('cron job completed')

        except Exception as e:
            print(e)

        finally:
            self.cursor.close()
            self.connection.close()
        end = time.time()
        total = end - start
        print(f'time taken by single cron {total}')

    def storeScrapeInDb(self,row):
        getRankQuery = "select `rank` from rank_table where asinId = %s and keyword = %s and recordedAt = %s"
        self.cursor.execute(getRankQuery, (row[0], row[1], date.today()))
        res = self.cursor.fetchone()
        current_rank_json = res['rank'] if res else None

        if current_rank_json:
            print("updating")
            current_rank_array = json.loads(current_rank_json)
            current_rank_array.append(row[2])
            updated_rank_json = json.dumps(current_rank_array)
            updateQuery = "UPDATE rank_table SET `rank` = %s WHERE asinId = %s and keyword = %s and recordedAt = %s"
            self.cursor.execute(updateQuery,(updated_rank_json, row[0], row[1], date.today()))
            self.connection.commit()
        else:
            print('inserting the record')
            insertQuery = "Insert into rank_table (asinId, keyword, `rank`, recordedAt) values (%s,%s,%s,%s)"
            self.cursor.execute(insertQuery, (row[0], row[1], json.dumps([row[2]]), date.today()))
            self.connection.commit()

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
        print(data)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.invoke_lambda_function, payload) for payload in data]
            while futures:
                completed, _ = concurrent.futures.wait(futures, timeout=0)
                for future in completed:
                    result = future.result()
                    rows = self.fetchScrapeData(result)
                    for row in rows:
                        if row:
                            self.storeScrapeInDb(row)

                    futures.remove(future)
        

    def fetchScrapeData(self, result):
        status = result.get('statusCode', 0)
        if status == 200:
            data = json.loads(result.get('message'))['data']
            result = []
            for d in data:
                if len(d):
                    result.append([d.get('asin'), d.get('keyword'),d.get('rank')])
            return result
        else:
            return []
                


    def invoke_lambda_function(self, payload):
        start = time.time()
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
                    end = time.time()
                    total = end - start
                    print(f'time for {payload} --> {total}')
                    return res
                
                if retry > maxRetries:
                    break

                retry = retry + 1
                print('retrying',f'worker{randomWorker}')

            print('500 error payload-->', payload)
            end = time.time()
            total = end - start
            print(f'time for {payload} --> {total}')

            return {}
        
            # cases handled after applying retries may be status code 500

        except Exception as e:
            # other exception
            print('Lambda service exception for {payload}', e)
            end = time.time()
            total = end - start
            print(f'time for {payload} --> {total}')
            return {}


if __name__ == "__main__":
    app = Invoker()
    # runs for every 5 minutes
    schedule.every(3).minutes.do(app.main)
    while True:
        schedule.run_pending()
        time.sleep(1)
    # app.main()
