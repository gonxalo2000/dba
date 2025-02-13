select owner, object_name, object_type, created
from dba_objects
WHERE owner LIKE 'DBA%'
	and object_name LIKE 'D%'
