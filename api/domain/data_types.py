from typing import List


class DataTypes:
    DATE = "date"
    INT = "Int64"
    FLOAT = "Float64"
    STRING = "object"
    OBJECT = "object"
    BOOLEAN = "boolean"

    @classmethod
    def accepted_data_types(cls) -> List[str]:
        return [cls.DATE, cls.INT, cls.FLOAT, cls.STRING, cls.OBJECT, cls.BOOLEAN]

    @classmethod
    def numeric_data_types(cls) -> List[str]:
        return [cls.INT, cls.FLOAT]

    @classmethod
    def data_types_to_cast(cls) -> List[str]:
        return [cls.INT, cls.FLOAT, cls.BOOLEAN]

    @classmethod
    def custom_data_types(cls) -> List[str]:
        return [cls.DATE]
