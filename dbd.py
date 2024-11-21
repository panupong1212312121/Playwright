############################################
# Clean diw datas
############################################

import pandas as pd

# Import diw data
# input_data_path = './diw/all/diw_client.xlsx'
input_data_path = './diw_client.xlsx'
df_template = pd.read_excel(input_data_path)

df = df_template.copy()

# remove ผู้ประกอบการว่าง
df = df.dropna(subset=['ผู้ประกอบการ'])

# Clean data before extract
pattern = r'บริษัท|จำกัด|ห้างหุ้นส่วน|สามัญนิติบุคคล|หนังสือบริคณห์สนธิ|มหาชน|หอการค้า|สมาคมการค้า|จังหวัด|\([^\(\)]*\)'
df['ผู้ประกอบการ'] = df['ผู้ประกอบการ'].str.replace(pattern,'',regex=True).str.strip()

# Sort data before extract
sorter = ['กทม. และภาคกลาง','ภาคตะวันออก','ภาคตะวันตก','ภาคเหนือ','ภาคใต้','ภาคตะวันออกเฉียงเหนือ']

df['Area'] = df['Area'].astype("category")
df['Area'] = df['Area'].cat.reorder_categories(sorter)
df = df.sort_values(['Area']).reset_index(drop=True)

############################################
# Extract all dbd datas
############################################

from playwright.async_api import async_playwright
from datetime import datetime
from bs4 import BeautifulSoup
import logging 
import re
import asyncio

def create_log(log_name,log_output_path):
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)

    filehandler = logging.FileHandler(log_output_path)
    filehandler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")
    )
    logger.addHandler(filehandler)

    return logger

def export_file(curr_arr,curr_idx,column_name,export_name,export_path):
    curr_arr.append(curr_idx)
    
    dbd_df = pd.DataFrame(curr_arr,columns=column_name)
    
    dbd_df_file = f'{export_name}.csv'
    dbd_df_path = f'{export_path}/{dbd_df_file}'

    dbd_df.to_csv(dbd_df_path)

async def main():

    ############### Config ##################
    # start_idx = 6610 
    start_idx = 0
    end_idx = len(df)-1

    track_idx_logger_name = 'idx' 
    track_url_logger_name = 'url'
    track_click_logger_name = 'click'
    track_data_logger_name = 'data'

    track_status_logger_name = 'status'

    dbd_success_path = './dbd/success'
    dbd_fail_path = './dbd/fail'

    dbd_data_path = './dbd/data'

    dbd_error_path = './dbd/error'
    dbd_no_income_statement_path = './dbd/no_income_statement'
    dbd_cannot_get_financial_statement_table_path = './dbd/cannot_get_financial_statement_table'
    dbd_cannot_access_income_statement_path = './dbd/cannot_access_income_statement'

    #########################################

    start_time = datetime.now()

    success_idx = []
    fail_idx = []

    data = []

    error_idx = []
    no_income_statement_idx = []
    cannot_get_financial_statement_table_idx = []
    cannot_access_income_statement_idx = []
    

    column_name = ['idx',
                   'เลขทะเบียนนิติบุคคล',
                   'เลขทะเบียนโรงงาน',
                   'ชื่อนิติบุคคล',
                   'งบกำไรขาดทุน',
                   'ปี',
                   'จำนวนเงิน',
                   'การเปลี่ยนแปลง',
                   'ประเภทนิติบุคคล',
                   'สถานะนิติบุคคล',
                   'วันที่จดทะเบียนจัดตั้ง',
                   'ทุนจดทะเบียน',
                   'เลขทะเบียนเดิม',
                   'กลุ่มธุรกิจ',
                   'ขนาดธุรกิจ',
                   'ที่ตั้งสำนักงานแห่งใหญ่',
                   'ที่ตั้งตรงกับสำนักงานใหญ่',
                   'หาเจอ',
                    ]
    
    idx_name = ['idx']

    find_again_url = 'https://datawarehouse.dbd.go.th/searchJuristicInfo'

    reg_province = r'(?<=จ\.)\s*[\u0E00-\u0e7f]+|กรุงเทพมหานคร'
    reg_district = r'(?<=อ\.)\s*[\u0E00-\u0e7f]+|(?<=เขต)\s*[\u0E00-\u0e7f]+'
    reg_subdistrict = r'(?<=ต\.)\s*[\u0E00-\u0e7f]+|(?<=แขวง)\s*[\u0E00-\u0e7f]+'

    # Create log
    num_dash = 100

    track_idx_logger = create_log(f'{track_idx_logger_name}',f'./log/{track_idx_logger_name}.log')
    track_url_logger = create_log(f'{track_url_logger_name}',f'./log/{track_url_logger_name}.log')
    track_click_logger = create_log(f'{track_click_logger_name}',f'./log/{track_click_logger_name}.log')
    track_data_logger = create_log(f'{track_data_logger_name}',f'./log/{track_data_logger_name}.log')
    track_status_logger = create_log(f'{track_status_logger_name}',f'./log/{track_status_logger_name}.log')

    async with async_playwright() as playwright:
        # Launch a browser
        browser = await playwright.chromium.launch(headless=False) 

        # Create a new page
        page = await browser.new_page()

        start_url = 'https://datawarehouse.dbd.go.th/index'

        previous_url = start_url
        
        await page.goto(start_url)
        track_url_logger.debug(f'Completed goto {start_url}')

        # กด button ปิด , ยอมรับทั้งหมด
        await page.locator('//button[text()="ปิด"]').click() 
        track_click_logger.debug('Completed Click "ปิด"')
        
        await page.locator('//button[text()="ยอมรับทั้งหมด"]').click()
        track_click_logger.debug('Completed Click "ยอมรับทั้งหมด"')

        # Loop ข้อมูลทีละ idx 
        for idx in range(start_idx,end_idx+1):
            broken_web = True
            extract_income_statement = False
            exitLoop = False
            
            # Run again when broken_web
            while broken_web:
                # ถ้า run แล้วเว็บไม่มีปัญหา
                try:
                    track_idx_logger.debug(f'{idx = }')
                    track_url_logger.debug(f'{idx = }')
                    track_click_logger.debug(f'{idx = }')
                    track_data_logger.debug(f'{idx = }')
                    track_status_logger.debug(f'{idx = }')

                    factory_id = df.loc[idx,'เลขทะเบียนโรงงาน']

                    client = df.loc[idx,'ผู้ประกอบการ'].strip()
                    province = df.loc[idx,'จังหวัด'].strip()
                    district = df.loc[idx,'อำเภอ'].strip()
                    subdistrict = df.loc[idx,'ตำบล'].strip()

                    # กรณี search ครั้งแรก
                    if idx==start_idx:
                        input = page.get_by_placeholder('ค้นหาด้วยชื่อหรือเลขทะเบียนนิติบุคคล รหัสประเภทธุรกิจ ชื่อหรือคำอธิบายประเภทธุรกิจ')
                    # กรณี search ครั้งต่อไป
                    else:
                        input = page.get_by_placeholder('ค้นหาด้วยชื่อหรือเลขทะเบียนนิติบุคคล')

                    await input.fill(client)
                    await input.press('Enter') 

                    track_url_logger.debug(f'Completed fill {client = }')

                    current_url = page.url

                    track_url_logger.debug(f'{previous_url = }')
                    track_url_logger.debug(f'{current_url = }')

                    track_status_logger.debug(f'Completed fill {client = } & Press "Enter"')
                    
                    # กรณี search ครั้งเดียวเจอ , url เปลี่ยนตลอด 
                    if current_url != find_again_url and current_url != previous_url:
                        
                        parent = "//*[@id='companyProfileTab1']/div[2]/div[1]/div[1]/div/div/div"

                        corporation_type = await page.locator(f"{parent}/div[text()='ประเภทนิติบุคคล']/following-sibling::div[1]").text_content()

                        corporation_status = await page.locator(f"{parent}/div[text()='สถานะนิติบุคคล']/following-sibling::div[1]").text_content()

                        registration_date = await page.locator(f"{parent}/div[text()='วันที่จดทะเบียนจัดตั้ง']/following-sibling::div[1]").text_content()

                        registered_capital = await page.locator(f"{parent}/div[text()='ทุนจดทะเบียน']/following-sibling::div[1]").text_content()

                        old_corporation_id = await page.locator(f"{parent}/div[text()='เลขทะเบียนเดิม']/following-sibling::div[1]").text_content()

                        business_type = await page.locator(f"{parent}/div[text()='กลุ่มธุรกิจ']/following-sibling::div[1]").text_content()

                        business_size = await page.locator(f"{parent}/div[text()='ขนาดธุรกิจ']/following-sibling::div[1]").text_content()

                        center_location = await page.locator(f"{parent}/div[text()='ที่ตั้งสำนักงานแห่งใหญ่']/following-sibling::div[1]").text_content()

                        found = 'Yes'

                        # check ที่ตั้งสำนักงานใหญ่ 
                        if province == re.findall(reg_province,center_location)[0].strip() and \
                            district == re.findall(reg_district,center_location)[0].strip() and \
                            subdistrict == re.findall(reg_subdistrict,center_location)[0].strip():
                            same_center_location = 'Yes'

                        else:
                            same_center_location = 'No'

                        await page.locator('//span[text()="ข้อมูลงบการเงิน"]').click()
                        track_click_logger.debug('Completed Click "ข้อมูลงบการเงิน"')

                        # Define Boolean broken_web
                        broken_web = False

                        # check ว่ามีข้อมูลงบกำไรขาดทุนไหม ถ้ามีดึงมันมา 
                        try:
                            await page.locator('//span[text()="งบกำไรขาดทุน"]').click()
                            track_click_logger.debug('Completed Click "งบกำไรขาดทุน"')
    
                            track_status_logger.debug(f'Has income statement')

                            # check ว่าสามารถดึงงบกำไรขาดทุนมาได้ไหม ถ้าได้ดึงมาปกติ ถ้าไม่ได้ให้ดึงอีกครั้ง
                            while not extract_income_statement:
                                try:
                                    # locate ไปที่ income statement table 
                                    table_loc = page.locator("(//div[@class='table-responsive'])[1]")
                                    table = BeautifulSoup(await table_loc.inner_html(), 'html.parser')
                                    track_data_logger.debug(f'Completed Access table')

                                    # current year - 1 นับถอยหลังไป 5 
                                    year_loc = page.locator("//table[@class='table table-hover text-end table-fixed']/thead/tr[1]")
                                    year = BeautifulSoup(await year_loc.inner_html(), 'html.parser')
                                    track_data_logger.debug(f'Completed Access year')
                                
                                    years = []

                                    for y in year.find_all('th'):
                                        years.append(y.text)

                                    years = years[1:]

                                    corporation_id = await page.locator("//h4[contains(text(),'เลขทะเบียนนิติบุคคล')]").text_content()
                                    corporation_id = corporation_id.split(':')[1]

                                    corporation_name = await page.locator("//h3[contains(text(),'ชื่อนิติบุคคล')]").text_content()
                                    corporation_name = corporation_name.split(':')[1]

                                    track_status_logger.debug(f'can_get_financial_statement_table')

                                    for body in table.find_all('tbody'):
                                        for rows in body.find_all('tr'):
                                            for income_states in rows.find_all('th'):
                                                # check ว่าสามารถดึงงบกำไรขาดทุนมาได้ไหม ถ้าได้ดึงมาปกติ ถ้าไม่ได้ให้ดึงอีกครั้ง
                                                if income_states.text == 'รายได้หลัก':
                                                    # Define Boolean extract_income_statement
                                                    extract_income_statement = True
                                                
                                                if extract_income_statement:
                                                    for i,year_state in enumerate(years):
                                                        values = rows.find_all('td')[2*i:2*(i+1)]
                                                        amount = values[0].text
                                                        change_amount = values[1].text

                                                        data.append([idx,
                                                                    corporation_id,
                                                                    factory_id,
                                                                    corporation_name,
                                                                    income_states.text,
                                                                    year_state,
                                                                    amount,
                                                                    change_amount,
                                                                    corporation_type,
                                                                    corporation_status,
                                                                    registration_date,
                                                                    registered_capital,
                                                                    old_corporation_id,
                                                                    business_type,
                                                                    business_size,
                                                                    center_location,
                                                                    same_center_location,
                                                                    found])

                                                else:
                                                    exitLoop = True
                                                    break
                                            if exitLoop:
                                                break
                                        if exitLoop:
                                            break

                                    if extract_income_statement:
                                        track_data_logger.debug(f'Completed Extract Income Statement')
                                        
                                        # Export success_idx
                                        export_file(success_idx,
                                                    idx,
                                                    idx_name,
                                                    f'success_idx {start_idx}',
                                                    dbd_success_path)
                                        
                                        track_status_logger.debug(f'can_access_income_statement')

                                    else:
                                        # Export cannot_access_income_statement_idx
                                        export_file(cannot_access_income_statement_idx,
                                                idx,
                                                idx_name,
                                                f'cannot_access_income_statement_idx {start_idx}'
                                                ,dbd_cannot_access_income_statement_path)
                                        
                                        # Export fail_idx
                                        export_file(fail_idx,
                                                    idx,
                                                    idx_name,
                                                    f'fail_idx {start_idx}'
                                                    ,dbd_fail_path)
                                        
                                        track_status_logger.error(f'cannot_access_income_statement')

                                except:
                                    # Export cannot_get_financial_statement_table_idx
                                    export_file(cannot_get_financial_statement_table_idx,
                                                idx,
                                                idx_name,
                                                f'cannot_get_financial_statement_table_idx {start_idx}'
                                                ,dbd_cannot_get_financial_statement_table_path)

                                    # Export fail_idx
                                    export_file(fail_idx,
                                                idx,
                                                idx_name,
                                                f'fail_idx {start_idx}'
                                                ,dbd_fail_path)
                                    
                                    track_status_logger.error(f'cannot_get_financial_statement_table')

                        except:
                            # Export no_income_statement_idx
                            export_file(no_income_statement_idx,
                                        idx,
                                        idx_name,
                                        f'no_income_statement_idx {start_idx}'
                                        ,dbd_no_income_statement_path)

                            # Export success_idx
                            export_file(success_idx,
                                        idx,
                                        idx_name,
                                        f'success_idx {start_idx}',
                                        dbd_success_path)
                            
                            data.append([idx,
                                        '',
                                        factory_id,
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        corporation_type,
                                        corporation_status,
                                        registration_date,
                                        registered_capital,
                                        old_corporation_id,
                                        business_type,
                                        business_size,
                                        center_location,
                                        same_center_location,
                                        found])
                            
                            track_status_logger.error(f'No income statement')
                            
                    # กรณี search ครั้งเดียวไม่เจอ , url จะไม่เปลี่ยน 
                    else:
                        data.append([idx]+['']+[factory_id]+['']*(len(column_name)-3)+['No'])

                        # Export success_idx
                        export_file(success_idx,
                                    idx,
                                    idx_name,
                                    f'success_idx {start_idx}',
                                    dbd_success_path)

                    previous_url = current_url

                    # Export dbd data (Error this definitely)
                    dbd_df = pd.DataFrame(data,columns=column_name)
                    
                    dbd_df_file = f'dbd_client {start_idx}.csv'
                    dbd_df_path = f'{dbd_data_path}/{dbd_df_file}'

                    dbd_df.to_csv(dbd_df_path)

                    if extract_income_statement:
                        track_status_logger.info('Success') 
                    else:
                        track_status_logger.error('Fail') 
                    
                    track_idx_logger.debug('-'*num_dash)
                    track_url_logger.debug('-'*num_dash)
                    track_click_logger.debug('-'*num_dash)
                    track_data_logger.debug('-'*num_dash)
                    track_status_logger.debug('-'*num_dash)

                # ถ้า run แล้วเว็บมีปัญหา
                except:                    
                    # Export error_idx
                    export_file(error_idx,
                                idx,
                                idx_name,
                                f'error_idx {start_idx}',
                                dbd_error_path)        

                    # Export fail_idx
                    export_file(fail_idx,
                                idx,
                                idx_name,
                                f'fail_idx {start_idx}',
                                dbd_fail_path)
                    
                    track_status_logger.error('Fail') 
            
                    track_idx_logger.debug('-'*num_dash)
                    track_url_logger.debug('-'*num_dash)
                    track_click_logger.debug('-'*num_dash)
                    track_data_logger.debug('-'*num_dash)
                    track_status_logger.debug('-'*num_dash)

        await browser.close() 
            
        end_time = datetime.now()
        diff_time = end_time - start_time
        print(diff_time)

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())