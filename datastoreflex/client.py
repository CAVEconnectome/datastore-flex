from typing import Any
from google.cloud import datastore
from cloudfiles import CloudFiles
from cloudfiles.exceptions import UnsupportedProtocolError

COMLUMN_CONFIG_KEY_NAME = "column"
COMLUMN_CONFIG_BUCKET = "path"
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
        super(DatastoreFlex, self).__init__(
            project=project,
            namespace=namespace,
            credentials=credentials,
            client_options=client_options,
            _http=_http,
            _use_grpc=_use_grpc,
        )
        self._config = {}
        self._get = self.get
        self._get_multi = self.get_multi
        self._put = self.put
        self._put_multi = self.put_multi

    def _read_config(self) -> None:
        from json import loads

        config_key = self.key(
            f"{self.namespace}_config",
            COMLUMN_CONFIG_KEY_NAME,
            namespace=self.namespace,
        )
        config = self.get(config_key)
        self._config[COMLUMN_CONFIG_KEY_NAME] = loads(config.get("value", "{}"))

    @property
    def config(self):
        if self._config is None:
            self._read_config()
        return self._config

    def _parse_entity(self, entity: datastore.Entity) -> None:
        column_config = self.config[COMLUMN_CONFIG_KEY_NAME]
        for column, config in column_config.items():
            bucket = config[COMLUMN_CONFIG_BUCKET]
            path_elements = []
            for e in config[COMLUMN_CONFIG_PATH_ELEMENTS]:
                try:
                    path_elements.append(entity[e])
                except KeyError:
                    # ignore if path element doesn't exist
                    return

            path = "/".join(path_elements)
            cf = CloudFiles(f"{column_path}/{path}")
            entity[column] = cf.get(entity.id)

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

        self._parse_entity(entity)
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
        for entity in entities:
            self._parse_entity(entity)
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
