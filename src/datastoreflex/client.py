"""
Extends default datastore client.
"""

import json
from os import getenv
from typing import Any, Iterable, Optional

from cloudfiles import CloudFiles
from google.cloud import datastore

COLUMN_CONFIG_KEY_NAME = "column"
COLUMN_CONFIG_BUCKET = "bucket_path"
COLUMN_CONFIG_PATH_ELEMENTS = "path_elements"


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
        self._secrets = None

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
        config_key = self.key(
            f"{self.namespace}_config",
            COLUMN_CONFIG_KEY_NAME,
            namespace=self.namespace,
        )
        config_entity = datastore.Entity(config_key)
        config_entity["value"] = json.dumps(config)
        self._put_multi([config_entity])
        self._config = None
        return config_entity

    def _read_config(self) -> None:
        config_key = self.key(
            f"{self.namespace}_config",
            COLUMN_CONFIG_KEY_NAME,
            namespace=self.namespace,
        )
        try:
            config = self._get_multi([config_key])[0]
            self._config[COLUMN_CONFIG_KEY_NAME] = json.loads(config.get("value", "{}"))
        except IndexError:
            self._config[COLUMN_CONFIG_KEY_NAME] = {}

    def _read_columns(self, entities: Iterable[datastore.Entity]) -> None:
        column_configs = self.config.get(COLUMN_CONFIG_KEY_NAME, {})
        for column, config in column_configs.items():
            files = _get_filespaths(entities, config[COLUMN_CONFIG_PATH_ELEMENTS])
            files = CloudFiles(config[COLUMN_CONFIG_BUCKET]).get(files)
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
        self._allocate_ids(entities)
        column_configs = self.config.get(COLUMN_CONFIG_KEY_NAME, {})
        for column, config in column_configs.items():
            files = _get_filespaths(
                entities, config[COLUMN_CONFIG_PATH_ELEMENTS], append_none=True
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
            CloudFiles(config[COLUMN_CONFIG_BUCKET]).puts(upload_files)

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

    def _allocate_ids(self, entities: Iterable[datastore.Entity]) -> None:
        # otherwise, entities won't have an ID to index before put
        # need them to have one for writing to the bucket
        unnamed_entities = [entity for entity in entities if entity.key.id is None]
        if len(unnamed_entities) > 0:
            # assuming they all share the same base, possibly not safe
            base_key = unnamed_entities[0].key
            ids = self.allocate_ids(base_key, len(unnamed_entities))
            for entity, key_id in zip(unnamed_entities, ids):
                entity.key = key_id


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
        elements.append(str(entity.key.id_or_name))
        files.append(None if append_none and non_existent else "/".join(elements))
    return files
