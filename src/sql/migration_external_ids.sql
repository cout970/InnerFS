-- migrate existing data
insert or ignore into external_ids (internal_id, type)
select id, 0
from files;

-- migrate existing data
insert or ignore into external_ids (internal_id, type)
select id, 1
from directory_entries;