from typing import Optional
from pydantic import BaseSettings


class Settings(BaseSettings):
    app_name: str = 'Music Links API'
    api_v1: str = '/api/v1'
    database_url: str = ''
    test_database_url: str = ''
    discogs_key: str = ''
    discogs_secret: str = ''
    discogs_token: str = ''
    token_url: str = ''
    authorize_url: str = ''
    access_token_url: str = ''
    token: str = ''
    secret: str = ''

    class Config:
        env_file = '.env'


settings = Settings()
