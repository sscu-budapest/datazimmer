from colassigner import ColAccessor


class CommonCols(ColAccessor):
    person_id = "person_id"
    object_id = "object_id"


class PersonsCols(ColAccessor):
    name = "name"
    age = "age"


class ObjectsCols(ColAccessor):
    size = "size"


class EventsCols(ColAccessor):
    success = "success"
