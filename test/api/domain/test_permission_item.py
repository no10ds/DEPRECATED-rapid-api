from api.domain.permission_item import PermissionItem


class TestPermissionItem:
    def test_permission_generation_for_type_and_sensitivity(self):
        permission_item = PermissionItem(
            perm_id="1",
            sensitivity="ALL",
            perm_type="READ"
        )
        assert permission_item.permission == "READ_ALL"

    def test_permission_generation_for_standalone_actions(self):
        permission_item = PermissionItem(
            perm_id="1",
            sensitivity=None,
            perm_type="USER_ADMIN"
        )
        assert permission_item.permission == "USER_ADMIN"
