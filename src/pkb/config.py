from pathlib import Path
import logging

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

    root_dir: Path = Path(__file__).parent.parent.parent.resolve()

    model_config = SettingsConfigDict(
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
    def output_dir(self) -> Path:
        return self.data_dir / "output"

    @property
    def cache_dir(self) -> Path:
        return self.root_dir / ".cache"

    @property
    def log_dir(self) -> Path:
        return self.root_dir / ".log"    


settings = Settings()

# Create the directories

def initialise_directories() -> None:
    for path in [
        settings.input_dir,
        settings.intermediate_dir,
        settings.output_dir,
        settings.cache_dir,
        settings.log_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)

initialise_directories()    

def configure_logger() -> logging.Logger:
    logger = logging.getLogger("luigi-interface")
    logger.setLevel(logging.DEBUG if settings.debug else logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    )

    file_handler = logging.FileHandler(settings.log_dir / "error.log")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG if settings.debug else logging.INFO)
    logger.addHandler(console_handler)

    return logger


logger = configure_logger()