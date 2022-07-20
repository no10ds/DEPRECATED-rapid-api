from api.domain.permission_item import PermissionItem


class TestPermissionItem:
    def test_permission_generation_for_type_and_sensitivity(self):
        permission_item = PermissionItem(
            id="1",
            sensitivity="ALL",
            type="READ"
        )
        assert permission_item.generate_permission() == "READ_ALL"

    def test_permission_generation_for_standalone_actions(self):
        permission_item = PermissionItem(
            id="1",
            sensitivity=None,
            type="USER_ADMIN"
        )
        assert permission_item.generate_permission() == "USER_ADMIN"
