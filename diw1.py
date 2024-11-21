############################################
# Extract all diw datas
############################################

from playwright.async_api import async_playwright
from datetime import datetime
import asyncio

area_skip = []
province_skip = []

async def main():
    start_time = datetime.now()
    broken_excel_file = []
    async with async_playwright() as playwright:
        # Launch a browser
        browser = await playwright.chromium.launch(headless=False) # headless=False : ให้หน้า browser ขึ้นมาเวลารันเป็นเวลา slow_mo=500 : pop-up ขึ้นมาเป็นเวลา 0.5 secs
        # Create a new page
        page = await browser.new_page()

        target_url = 'https://userdb.diw.go.th/factoryPublic/tumbol.asp'

        await page.goto(target_url)

        area_arr = []

        area = await page.locator('//a').all()

        for a in area:
            area_arr.append(await a.text_content())

        # Loop area
        for a in area_arr:
            if a not in area_skip:
                await page.locator(f"//a[text()='{a}']").click()

                await page.reload()

                province_arr = []

                province = await page.locator("//a[contains(text(),'จ.')]").all()

                for p in province:
                    province_arr.append(await p.text_content())

                # Loop province
                for p in province_arr:
                    if p not in province_skip:
                        await page.locator(f"//a[text()='{p}']").click()

                        await page.reload()

                        district_arr = []

                        district = await page.locator("//a[contains(text(),'อ.')]").all()

                        for d in district:
                            district_arr.append(await d.text_content())

                        # Loop district
                        for d in district_arr:
                            if 'กิ่งอำเภอ' not in d:
                                await page.locator(f"//a[text()='{d}']").click()

                                # await page.reload()

                                # Start waiting for the download
                                async with page.expect_download() as download_info:
                                    # Perform the action that initiates download
                                        await page.locator(f"//a[contains(text(),'download')]").click()

                                        download = await download_info.value

                                        # Wait for the download process to complete and save the downloaded file somewhere
                                        await download.save_as(f"./diw/{a}/{download.suggested_filename}")
                                        await page.reload()
                            else:
                                broken_excel_file.append([a,p,d])
                                await page.reload()  # Skip to the next download

        await browser.close() 

        end_time = datetime.now()
        diff_time = end_time - start_time
        print(diff_time)

if __name__ == "__main__":
    # asyncio.run(main())
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())