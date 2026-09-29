from pathlib import Path
import logging

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    debug: bool = True

    graph_backend: str = "neo4j"

    graph_url: str = "ws://localhost:8182/gremlin"
    graph_traversal_source: str = "g"

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    neo4j_database: str = "neo4j" 

    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_database: str = "pkb"
    postgres_user: str = "pkb"
    postgres_password: str = "pkb"

    aws_region: str = Field(
        default="eu-west-2",
        validation_alias=AliasChoices("PKB_AWS_REGION", "AWS_REGION"),
    )
    aws_access_key_id: str | None = Field(
        default=None,
        validation_alias="AWS_ACCESS_KEY_ID",
    )
    aws_secret_access_key: str | None = Field(
        default=None,
        validation_alias="AWS_SECRET_ACCESS_KEY",
    )
    s3_endpoint_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("PKB_S3_ENDPOINT_URL", "S3_ENDPOINT_URL"),
    )
    source_data_bucket: str = "pkb-source-data"

    root_dir: Path = Path(__file__).parent.parent.parent.resolve()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PKB_",
        extra="ignore",
    )

    @property
    def data_dir(self) -> Path:
        return self.root_dir / "data"

    @property
    def input_dir(self) -> Path:
        return self.data_dir / "input"

    @property
    def intermediate_dir(self) -> Path:
        return self.data_dir / "intermediate"
    
    @property
    def assets_dir(self) -> Path:
        return self.data_dir / "assets"    

    @property
    def output_dir(self) -> Path:
        return self.data_dir / "output"

    @property
    def cache_dir(self) -> Path:
        return self.root_dir / ".cache"


settings = Settings()

# Create the directories


def initialise_directories() -> None:
    for path in [
        settings.input_dir,
        settings.assets_dir,
        settings.intermediate_dir,
        settings.output_dir,
        settings.cache_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)

initialise_directories()


def configure_logger() -> logging.Logger:
    logger = logging.getLogger("pkb")
    logger.setLevel(logging.DEBUG if settings.debug else logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(handler)

    logger.propagate = False
    return logger


logger = configure_logger()
