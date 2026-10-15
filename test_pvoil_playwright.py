
from pathlib import Path
from playwright.sync_api import sync_playwright

URL = "https://www.pvoil.com.vn/tin-gia-xang-dau"
PROFILE = Path("browser_profiles/pvoil_edge").resolve()
PROFILE.mkdir(parents=True, exist_ok=True)

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE),
        channel="msedge",
        headless=False,
        locale="vi-VN",
        timezone_id="Asia/Ho_Chi_Minh",
    )

    page = context.pages[0] if context.pages else context.new_page()

    try:
        page.goto(
            URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )

        print("Nếu Cloudflare yêu cầu xác minh, hãy hoàn tất trong Edge.")
        input("Khi trang PVOIL đã hiện danh sách ngày, nhấn Enter...")

        options = page.locator("#ddlpricedate option")
        count = options.count()

        print("TOTAL PERIODS:", count)

        if count:
            print("NEWEST:", options.first.get_attribute("value"))
            print("OLDEST:", options.last.get_attribute("value"))
        else:
            print("Không tìm thấy danh sách kỳ điều chỉnh.")

    finally:
        context.close()
