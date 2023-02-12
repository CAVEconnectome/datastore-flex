"""
Extends default datastore client.
"""

from typing import Any
from typing import Iterable
from typing import Optional
from os import getenv

from google.cloud import datastore
from cloudfiles import CloudFiles

COMLUMN_CONFIG_KEY_NAME = "column"
COMLUMN_CONFIG_BUCKET = "bucket_path"
COMLUMN_CONFIG_PATH_ELEMENTS = "path_elements"


class DatastoreFlex(datastore.Client):
    def __init__(
        self,
        project: str = None,
        namespace: str = None,
        credentials: Any = None,
        client_options: Any = None,
        _http: Any = None,
        _use_grpc: bool = None,
    ):
        parent = super()
        parent.__init__(
            project=project,
            namespace=namespace,
            credentials=credentials,
            client_options=client_options,
            _http=_http,
            _use_grpc=_use_grpc,
        )
        self._config = None

        # datastore client uses multi versions for `get` and `put` internally
        # this leads to recursion if `get` and `put` are overidden
        self._get_multi = parent.get_multi
        self._put_multi = parent.put_multi

    @property
    def config(self):
        if self._config is None:
            self._config = {}
            self._read_config()
        return self._config

    def add_config(self, config: dict = {}) -> datastore.Entity:
        from json import dumps

        config_key = self.key(
            f"{self.namespace}_config",
            COMLUMN_CONFIG_KEY_NAME,
            namespace=self.namespace,
        )
        config_entity = datastore.Entity(config_key)
        config_entity["value"] = dumps(config)
        self._put_multi([config_entity])
        self._config = None
        return config_entity

    def _read_config(self) -> None:
        from json import loads

        config_key = self.key(
            f"{self.namespace}_config",
            COMLUMN_CONFIG_KEY_NAME,
            namespace=self.namespace,
        )
        try:
            config = self._get_multi([config_key])[0]
            self._config[COMLUMN_CONFIG_KEY_NAME] = loads(config.get("value", "{}"))
        except IndexError:
            self._config[COMLUMN_CONFIG_KEY_NAME] = {}

    def _read_columns(self, entities: Iterable[datastore.Entity]) -> None:
        column_configs = self.config.get(COMLUMN_CONFIG_KEY_NAME, {})
        for column, config in column_configs.items():
            files = _get_filespaths(entities, config[COMLUMN_CONFIG_PATH_ELEMENTS])
            cf = CloudFiles(config[COMLUMN_CONFIG_BUCKET])
            files = cf.get(files)
            for entity, file_content in zip(entities, files):
                if file_content["error"] is not None:
                    continue
                entity[column] = file_content["content"]

    def _write_columns(
        self,
        entities: Iterable[datastore.Entity],
        compression: str,
        compression_level: int,
    ) -> None:
        column_configs = self.config.get(COMLUMN_CONFIG_KEY_NAME, {})
        for column, config in column_configs.items():
            files = _get_filespaths(
                entities, config[COMLUMN_CONFIG_PATH_ELEMENTS], append_none=True
            )
            upload_files = []
            for entity, file_path in zip(entities, files):
                if file_path is None:
                    continue
                try:
                    file_d = {
                        "content": entity[column],
                        "path": file_path,
                        "compress": compression,
                        "compression_level": compression_level,
                        "cache_control": getenv(
                            "CACHE_CONTROL",
                            "public; max-age=3600",
                        ),
                    }
                    upload_files.append(file_d)
                except KeyError:
                    continue
                entity.pop(column, None)
            cf = CloudFiles(config[COMLUMN_CONFIG_BUCKET])
            cf.puts(upload_files)

    def get(
        self,
        key,
        missing=None,
        deferred=None,
        transaction=None,
        eventual=False,
        retry=None,
        timeout=None,
    ) -> datastore.Entity:
        entities = self._get_multi(
            keys=[key],
            missing=missing,
            deferred=deferred,
            transaction=transaction,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
        )
        if entities:
            entity = entities[0]
        else:
            return None

        self._read_columns([entity])
        return entity

    def get_multi(
        self,
        keys,
        missing=None,
        deferred=None,
        transaction=None,
        eventual=False,
        retry=None,
        timeout=None,
    ) -> Iterable[datastore.Entity]:
        entities = self._get_multi(
            keys=keys,
            missing=missing,
            deferred=deferred,
            transaction=transaction,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
        )
        self._read_columns(entities)
        return entities

    def put(
        self,
        entity,
        retry=None,
        timeout=None,
        compression: Optional[str] = "gzip",
        compression_level: Optional[int] = 6,
    ) -> None:
        self._write_columns(
            [entity],
            compression=compression,
            compression_level=compression_level,
        )
        self._put_multi([entity], retry, timeout)

    def put_multi(
        self,
        entities,
        retry=None,
        timeout=None,
        compression: Optional[str] = "gzip",
        compression_level: Optional[int] = 6,
    ) -> None:
        self._write_columns(
            entities,
            compression=compression,
            compression_level=compression_level,
        )
        self._put_multi(entities, retry, timeout)


def _get_filespaths(
    entities: Iterable[datastore.Entity],
    path_elements: Iterable[str],
    append_none: bool = False,
) -> Iterable[str]:
    """
    `append_none` is used to determine whether a file needs to be written in `put` and `put_multi`
    """
    files = []
    for entity in entities:
        non_existent = False
        elements = []
        for element in path_elements:
            try:
                elements.append(entity[element])
            except KeyError:
                # ignore if path element doesn't exist
                # cloudfiles will error and return None
                elements.append("non_existent")
                non_existent = True
        elements.append(entity.key.id_or_name)
        files.append(None if append_none and non_existent else "/".join(elements))
    return files
