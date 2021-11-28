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
        self._config = {}
        self._get = parent.get
        self._get_multi = parent.get_multi
        self._put = parent.put
        self._put_multi = parent.put_multi

    @property
    def config(self):
        if self._config is None:
            self._read_config()
        return self._config

    def _read_config(self) -> None:
        from json import loads

        config_key = self.key(
            f"{self.namespace}_config",
            COMLUMN_CONFIG_KEY_NAME,
            namespace=self.namespace,
        )
        config = self.get(config_key)
        self._config[COMLUMN_CONFIG_KEY_NAME] = loads(config.get("value", "{}"))

    def _read_columns(self, entities: Iterable[datastore.Entity]) -> None:
        column_configs = self.config[COMLUMN_CONFIG_KEY_NAME]
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
        column_configs = self.config[COMLUMN_CONFIG_KEY_NAME]
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
        entity = self._get(
            key=key,
            missing=missing,
            deferred=deferred,
            transaction=transaction,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
        )

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
        # write to datastore first, higher priority
        self._put(entity, retry, timeout)
        self._write_columns(
            [entity],
            compression=compression,
            compression_level=compression_level,
        )

    def put_multi(
        self,
        entities,
        retry=None,
        timeout=None,
        compression: Optional[str] = "gzip",
        compression_level: Optional[int] = 6,
    ) -> None:
        # write to datastore first, higher priority
        self._put_multi(entities, retry, timeout)
        self._write_columns(
            entities,
            compression=compression,
            compression_level=compression_level,
        )


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
        elements.append(entity.id)
        files.append(None if append_none and non_existent else "/".join(elements))
    return files
