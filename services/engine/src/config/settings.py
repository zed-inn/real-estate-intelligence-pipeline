from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str = "localhost:9092"
    kafka_group_id: str = "real-estate-intelligence-engine-group"
    
    class Config:
        env_file = ".env"

settings = Settings()
