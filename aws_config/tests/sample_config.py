# -*- coding: utf-8 -*-

from pydantic import BaseModel, Field

from ..env import BaseEnv


class Server(BaseModel):
    ip: str | None = Field(default=None)
    cpu: int | None = Field(default=None)
    memory: int | None = Field(default=None)
    domain: str | None = Field(default=None)


class Servers(BaseModel):
    blue: Server | None = Field(default=None)
    green: Server | None = Field(default=None)
    black: Server | None = Field(default=None)
    white: Server | None = Field(default=None)


class Database(BaseModel):
    host: str | None = Field(default=None)
    port: int | None = Field(default=None)
    password: str | None = Field(default=None)


class Env(BaseEnv):
    username: str | None = Field(default=None)
    password: str | None = Field(default=None)
    tags: dict[str, str] = Field(default_factory=dict)
    servers: Servers | None = Field(default=None)
    databases: list[Database] = Field(default_factory=list)


data = {
    "project_name": "my_app",
    "env_name": "dev",
    "s3uri_data": "s3://myapp-dev/data/",
    "s3uri_artifacts": "s3://myapp-dev/artifacts/",
    "username": "alice@email.com",
    "password": "alicepassword",
    "servers": {
        "blue": {
            "ip": "111.111.111.111",
            "cpu": 4,
            "memory": 16,
            "domain": "blue.myapp.com",
        },
        "green": {
            "ip": "222.222.222.222",
        },
    },
    "databases": [
        {"host": "db1.myapp.com", "port": 5432, "password": "db1password"},
        {"host": "db2.myapp.com", "port": 3306, "password": "db2password"},
    ],
}
