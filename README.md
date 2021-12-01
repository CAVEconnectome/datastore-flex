##
Adds the ability to read/write objects in a cloud bucket to datastore client.

```
from datastoreflex import DatastoreFlex
client = DatastoreFlex(project="<project-id>", namespace="<namespace>")

# The following configuration means store a column/property named `v1`
# in a cloud bucket (protocol must be supported by cloudfiles).
# Value for `v1` will be stored in the path `gs://my_data_bucket/<group_id>/<user_id>`
# where <group_d> = entity["group_id"] and <user_id> = entity["user_id"]

config = {"v1": {"bucket_path": "gs://my_data_bucket", "path_elements": ["group_id", "user_id"]}}
client.add_config(config)
```
