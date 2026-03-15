# Data Dictionary — Glamira Analytics
**Database:** `glamira`
**Collection:** `summary`
**Total Records:** ~41,432,473 documents
**Data Period:** 2020 (primary)

---

## Common Fields (có trong hầu hết mọi event)

| Field | Type | Nullable | Description | Example |
|-------|------|----------|-------------|---------|
| `_id` | ObjectId | No | MongoDB auto-generated ID | `5ed8cb2bc671fc36b74653ad` |
| `time_stamp` | int | No | Unix timestamp (seconds) | `1591266092` |
| `ip` | str | No | IPv4 address của user | `37.170.17.183` |
| `user_agent` | str | No | Browser/device user agent string | `Mozilla/5.0 (iPhone...)` |
| `resolution` | str | No | Màn hình: `widthxheight` | `375x667` |
| `user_id_db` | str | Yes | ID user đã đăng nhập (rỗng nếu guest) | `502567` |
| `device_id` | str | No | UUID định danh thiết bị | `beb2cacb-20af-...` |
| `api_version` | str | No | Phiên bản API | `1.0` |
| `store_id` | str | No | ID store/quốc gia của Glamira | `12` |
| `local_time` | str | No | Thời gian local của user | `2020-06-04 12:21:27` |
| `show_recommendation` | str/None | Yes | Có hiển thị recommendation không | `"false"`, `"true"`, `None` |
| `current_url` | str | No | URL trang hiện tại | `https://www.glamira.fr/...` |
| `referrer_url` | str | Yes | URL trang trước đó | `https://www.google.com/` |
| `email_address` | str | Yes | Email user (rỗng nếu guest) | `user@example.com` |
| `collection` | str | No | Loại event/hành động | `view_product_detail` |

---

## Event Types & Fields Đặc Trưng

### 1. `view_product_detail` — 10,944,427 docs
User xem chi tiết 1 sản phẩm.

| Field | Type | Description |
|-------|------|-------------|
| `product_id` | str | ID sản phẩm đang xem |
| `recommendation` | bool | Có từ recommendation không |
| `utm_source` | bool | Có UTM source không,tham số tracking trong URL để biết traffic đến từ đâu |
| `utm_medium` | bool | Có UTM medium không |
| `option` | list[dict] | Các options sản phẩm đã chọn (alloy, diamond...) |

---

### 2. `select_product_option` — 8,844,342 docs
User chọn option (chất liệu, đá quý...) trên trang sản phẩm.

| Field | Type | Description |
|-------|------|-------------|
| `product_id` | str | ID sản phẩm |
| `option` | list[dict] | Option đã chọn: `option_label`, `option_id`, `value_label`, `value_id` |

---

### 3. `view_listing_page` — 11,259,694 docs
User xem trang danh sách sản phẩm (category page).

| Field | Type | Description |
|-------|------|-------------|
| `option` | dict | Filter đang áp dụng: `alloy`, `diamond`, `shapediamond` |
| `cat_id` | str/None | ID category |
| `collect_id` | str | ID collection |

---

### 4. `select_product_option_quality` — 2,231,825 docs
User chọn chất lượng/loại đá quý.

| Field | Type | Description |
|-------|------|-------------|
| `product_id` | str | ID sản phẩm |
| `option` | list[dict] | Option chất lượng được chọn |

---

### 5. `add_to_cart_action` — 187,901 docs
User thêm sản phẩm vào giỏ hàng.

| Field | Type | Description |
|-------|------|-------------|
| `product_id` | str | ID sản phẩm |
| `price` | str | Giá sản phẩm (có format tiền tệ) |
| `currency` | str | Ký hiệu tiền tệ |
| `is_paypal` | None/bool | Có dùng PayPal không |
| `option` | list[dict] | Options đã chọn khi add to cart |

---

### 6. `product_detail_recommendation_visible` — 1,302,362 docs
Recommendation hiển thị trên trang product detail.

| Field | Type | Description |
|-------|------|-------------|
| `viewing_product_id` | str | ID sản phẩm đang xem (dùng thay product_id) |

---

### 7. `product_detail_recommendation_noticed` — 490,780 docs
User chú ý đến recommendation trên trang product detail.

| Field | Type | Description |
|-------|------|-------------|
| `viewing_product_id` | str | ID sản phẩm đang xem |

---

### 8. `product_detail_recommendation_clicked` — 179,228 docs
User click vào 1 sản phẩm được recommend trên product detail.

| Field | Type | Description |
|-------|------|-------------|
| `viewing_product_id` | str | ID sản phẩm đang xem |
| `recommendation_product_id` | str | ID sản phẩm được click |
| `recommendation_clicked_position` | int | Vị trí của sản phẩm được click (0-indexed) |

---

### 9. `product_view_all_recommend_clicked` — 16,682 docs
User click "Xem tất cả" recommendation từ trang product.

| Field | Type | Description |
|-------|------|-------------|
| `viewing_product_id` | str | ID sản phẩm gốc đang xem |
| `recommendation_product_id` | str | ID sản phẩm được click trong list |
| `recommendation_product_position` | str | Vị trí trong danh sách |
| `referrer_url` | str | URL trang product gốc |

---

### 10. `view_shopping_cart` — 343,077 docs
User xem giỏ hàng.

| Field | Type | Description |
|-------|------|-------------|
| `cart_products` | list[dict] | Danh sách sản phẩm trong giỏ: `product_id`, `option` |

---

### 11. `checkout` — 88,540 docs
User đang ở bước thanh toán.

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | str | ID đơn hàng (có thể rỗng) |
| `cart_products` | list[dict] | Sản phẩm: `product_id`, `amount`, `option` |

---

### 12. `checkout_success` — 26,079 docs
User hoàn thành đặt hàng thành công.

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | int | ID đơn hàng thành công |
| `cart_products` | list[dict] | Sản phẩm: `product_id`, `amount`, `price`, `currency`, `option` |

---

### 13. `listing_page_recommendation_visible` — 718,048 docs
Recommendation hiển thị trên listing page.

| Field | Type | Description |
|-------|------|-------------|
| `option` | dict | Filter: `alloy`, `diamond`, `shapediamond` |
| `cat_id` | None | Category ID |
| `collect_id` | str | Collection ID |

---

### 14. `listing_page_recommendation_noticed` — 39,819 docs
User chú ý recommendation trên listing page.

| Field | Type | Description |
|-------|------|-------------|
| `option` | dict | Filter đang áp dụng |
| `cat_id` | None | Category ID |
| `collect_id` | str | Collection ID |

---

### 15. `listing_page_recommendation_clicked` — 25,545 docs
User click vào recommendation trên listing page.

| Field | Type | Description |
|-------|------|-------------|
| `recommendation_product_id` | None/str | ID sản phẩm được click |
| `recommendation_clicked_position` | None | Vị trí được click |
| `option` | dict | Filter: `alloy`, `diamond`, `shapediamond` |
| `cat_id` | None | Category ID |
| `collect_id` | str | Collection ID |

---

### 16. `view_landing_page` — 1,434,230 docs
User xem landing page (trang chiến dịch/quảng cáo).
> Chỉ có common fields, không có field đặc trưng.

---

### 17. `landing_page_recommendation_visible` — 314,999 docs
Recommendation hiển thị trên landing page.
> Chỉ có common fields.

---

### 18. `landing_page_recommendation_noticed` — 58,186 docs
User chú ý recommendation trên landing page.
> Chỉ có common fields.

---

### 19. `landing_page_recommendation_clicked` — 20,128 docs
User click recommendation trên landing page.

| Field | Type | Description |
|-------|------|-------------|
| `recommendation_product_id` | str | ID sản phẩm được click |
| `recommendation_product_position` | int | Vị trí trong danh sách (0-indexed) |

---

### 20. `view_home_page` — 1,053,420 docs
User xem trang chủ.
> Chỉ có common fields.

---

### 21. `view_static_page` — 1,451,565 docs
User xem trang tĩnh (order history, account...).
> Chỉ có common fields.

---

### 22. `view_my_account` — 112,066 docs
User xem trang tài khoản cá nhân.
> Chỉ có common fields.

---

### 23. `view_all_recommend` — 33,664 docs
User xem trang danh sách tất cả recommendation.

| Field | Type | Description |
|-------|------|-------------|
| `product_id` | str | ID sản phẩm gốc tạo ra recommendation list |
| `option` | dict | Options hiện tại: `alloy`, `stone`, `pearlcolor`, `finish`, `price` |

---

### 24. `search_box_action` — 238,308 docs
User thực hiện tìm kiếm.

| Field | Type | Description |
|-------|------|-------------|
| `key_search` | None/str | Từ khóa tìm kiếm |

---

### 25. `view_sorting_relevance` — 15,284 docs
User xem kết quả sắp xếp theo relevance.

| Field | Type | Description |
|-------|------|-------------|
| `option` | dict | Filter: `alloy`, `diamond`, `shapediamond` |

---

### 26. `sorting_relevance_click_action` — 1,713 docs
User click vào sản phẩm trong kết quả sorting relevance.

| Field | Type | Description |
|-------|------|-------------|
| `recommendation_product_id` | str | ID sản phẩm được click |
| `recommendation_product_position` | str | Vị trí trong danh sách |

---

### 27. `back_to_product_action` — 561 docs
User quay lại trang sản phẩm từ recommendation list.

| Field | Type | Description |
|-------|------|-------------|
| `product_id` | str | ID sản phẩm quay lại |

---

## Nested Object Structures

### `option` (dạng list[dict]) — dùng trong product events
```json
[
  {
    "option_label": "alloy",
    "option_id": "332084",
    "value_label": "yellow-375",
    "value_id": "3279318"
  },
  {
    "option_label": "diamond",
    "option_id": "",
    "value_label": "",
    "value_id": ""
  }
]
```

### `option` (dạng dict) — dùng trong listing/landing events
```json
{
  "alloy": "white-silber",
  "diamond": "",
  "shapediamond": ""
}
```

### `cart_products` (dùng trong cart/checkout events)
```json
[
  {
    "product_id": 97471,
    "amount": 1,
    "price": "880.00",
    "currency": "£",
    "option": [...]
  }
]
```

---

## Notes & Data Quality

| Vấn đề | Field | Ghi chú |
|--------|-------|---------|
| Field rỗng | `user_id_db`, `email_address` | Guest users không có giá trị |
| Nullable | `show_recommendation` | Có thể là `str`, `None` tùy event |
| Mixed type | `option` | Có thể là `list` hoặc `dict` tùy event |
| product_id thay thế | Một số events dùng `viewing_product_id` thay vì `product_id` | Cần xử lý khi query |
| `price` format | `add_to_cart_action` | Dạng string có ký tự đặc biệt, cần parse |
| `time_stamp` vs `local_time` | Cả 2 trường | `time_stamp` là UTC unix, `local_time` là giờ local của user |
