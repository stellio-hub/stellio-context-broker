match (e:Entity), (a:Attribute)
set e.createdAt = datetime(e.createdAt)
set e.modifiedAt = dateTime(e.modifiedAt)
set a.createdAt = datetime(a.createdAt)
set a.modifiedAt = dateTime(a.modifiedAt)
set a.observedAt = datetime(a.observedAt)
;
