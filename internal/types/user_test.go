package types

import "testing"

// TestUserListReqValidateAllowsTwoHundredRows 验证用户列表接受前端最大分页档位，并继续限制更大的直接请求。
func TestUserListReqValidateAllowsTwoHundredRows(t *testing.T) {
	req := &UserListReq{GetPageReq: GetPageReq{Page: 0, PageSize: userListMaxPageSize}}
	if err := req.Validate(); err != nil {
		t.Fatalf("UserListReq.Validate() error = %v", err)
	}
	if req.Page != defaultPageNumber || req.PageSize != userListMaxPageSize {
		t.Fatalf("用户列表分页 = %d/%d, want %d/%d", req.Page, req.PageSize, defaultPageNumber, userListMaxPageSize)
	}

	req = &UserListReq{GetPageReq: GetPageReq{Page: 2, PageSize: userListMaxPageSize + 1}}
	if err := req.Validate(); err != nil {
		t.Fatalf("UserListReq.Validate(oversized) error = %v", err)
	}
	if req.Page != 2 || req.PageSize != userListMaxPageSize {
		t.Fatalf("超限用户列表分页 = %d/%d, want 2/%d", req.Page, req.PageSize, userListMaxPageSize)
	}
}
