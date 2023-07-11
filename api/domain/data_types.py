from typing import List

# TODO: Are any of these necessary now?


# TODO: Do need some sort of check on partition data types. But the glue create table will probably do this anyway...?
class DataTypes:
    DATE = "date"
    INT = "integer"
    DOUBLE = "double"
    STRING = "string"
    OBJECT = "string"
    BOOLEAN = "boolean"

    @classmethod
    def accepted_data_types(cls) -> List[str]:
        return [cls.DATE, cls.INT, cls.DOUBLE, cls.STRING, cls.OBJECT, cls.BOOLEAN]

    @classmethod
    def data_types_to_cast(cls) -> List[str]:
        return [cls.INT, cls.DOUBLE, cls.BOOLEAN]

    @classmethod
    def custom_data_types(cls) -> List[str]:
        return [cls.DATE]
