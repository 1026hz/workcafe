from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kakao_api_key: str
    naver_client_id: str
    naver_client_secret: str
    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_db_name: str = "workcafe"
    redis_url: str = "redis://localhost:6379"

    class Config:
        env_file = ".env"


settings = Settings()
