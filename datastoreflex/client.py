from typing import Any
from typing import Iterable
from google.cloud import datastore
from cloudfiles.exceptions import UnsupportedProtocolError

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
        super().__init__(
            project=project,
            namespace=namespace,
            credentials=credentials,
            client_options=client_options,
            _http=_http,
            _use_grpc=_use_grpc,
        )
        self._config = {}
        self._get = datastore.Client.get
        self._get_multi = datastore.Client.get_multi
        self._put = datastore.Client.put
        self._put_multi = datastore.Client.put_multi

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

    def _get_filespaths(
        self,
        entities: Iterable[datastore.Entity],
        path_elements: Iterable[str],
    ) -> Iterable[str]:
        bucket = column_config[COMLUMN_CONFIG_BUCKET]
        files = []
        for entity in entities:
            elements = []
            for e in path_elements:
                try:
                    elements.append(entity[e])
                except KeyError:
                    # ignore if path element doesn't exist
                    # cloudfiles will error and return None
                    elements.append("non_existent")
            elements.append(entity.id)
            files.append("/".join(elements))
        return files

    def _fetch_columns(self, entities: Iterable[datastore.Entity]) -> None:
        from cloudfiles import CloudFiles

        column_configs = self.config[COMLUMN_CONFIG_KEY_NAME]
        for column, config in column_configs.items():
            files = self._get_filespaths(entities, config[COMLUMN_CONFIG_PATH_ELEMENTS])
            cf = CloudFiles(config[COMLUMN_CONFIG_BUCKET])
            files = cf.get(files)
            for entity, file_content in zip(entities, files):
                if file_content["error"] is not None:
                    continue
                entity[column] = file_content["content"]

    def get(
        self,
        key,
        missing=None,
        deferred=None,
        transaction=None,
        eventual=False,
        retry=None,
        timeout=None,
    ):
        entity = self._get(
            key,
            missing=missing,
            deferred=deferred,
            transaction=transaction,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
        )

        self._fetch_columns([entity])
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
    ):
        entities = self._get_multi(
            keys,
            missing=missing,
            deferred=deferred,
            transaction=transaction,
            eventual=eventual,
            retry=retry,
            timeout=timeout,
        )
        self._fetch_columns(entities)
        return entities

    def put(
        self,
        entity,
        retry=None,
        timeout=None,
    ):
        pass

    def put_multi(
        self,
        entities,
        retry=None,
        timeout=None,
    ):
        pass
