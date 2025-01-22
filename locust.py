import time, random
from locust import HttpUser, between, task, FastHttpUser

class WebSiteUser(FastHttpUser):
    wait_time = between(1,2)

    @task
    def send_vote(self):

        self.client.post(
            'sendToKafka',
            json={"name" : random.choice(["A","B","C"])}
            )
        

