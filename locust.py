import time, random
from locust import HttpUser, between, task

class WebSiteUser(HttpUser):
    wait_time = between(1,2)

    @task
    def home_page(self):
        self.client.get('/')

    @task
    def sendVote(self):

        self.client.post(
            'sendToKafka',
            json={"name" : random.choice(["A","B","C"])}
            )
        

