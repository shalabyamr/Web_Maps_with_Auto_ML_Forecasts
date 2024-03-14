from locust import HttpUser, task

class HelloWorldUser(HttpUser):
    @task
    def hello_world(self):
        self.client.get("/Users/amr/PycharmProjects/ggr_472_project/Maps/Folium_Toronto.html")
        self.client.get("/Users/amr/PycharmProjects/ggr_472_project/Maps/Mapbox_Pedestrian_HeatMap.html")