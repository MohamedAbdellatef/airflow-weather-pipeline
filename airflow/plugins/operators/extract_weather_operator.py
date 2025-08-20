from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests

class ExtractWeatherOperator(BaseOpperator):
    @apply_defaults
    def __inti__(self, city: str, api_key: str, *args, **kwargs):
        super(ExtractWeatherOperator,self).__init__(*args,**kwargs)
        self.city = city
        self.api_key = api_key
    def execute(self,context):
        url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={self.api_key}&units=metric"
        data = requests.get(url).json()
        self.log.info(data)

        #save to Bronze database later
        return data