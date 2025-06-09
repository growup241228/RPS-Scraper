import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
from datetime import datetime
import time
import logging
import os

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)

    sheet1 = client.open_by_url("https://docs.google.com/spreadsheets/d/1VyuRPidEfJkXk1xtn2uSmKGgcb8df90Wwx_TJ9qBLw0/edit?usp=sharing").worksheet("Sheet3")
    sheet2 = client.open_by_url("https://docs.google.com/spreadsheets/d/1yMXt8xPbnYL6_QF3Q37-WZKfv9EwIcQ1a3HAQu35TeE/edit?usp=sharing").worksheet("RPS_Closed")
    return sheet1, sheet2

def get_column_index(headers, target_name):
    for idx, col in enumerate(headers):
        if col.strip().lower() == target_name.strip().lower():
            return idx + 1  # gspread uses 1-based indexing
    raise ValueError(f"Column '{target_name}' not found. Headers were: {headers}")

def fetch_times_with_playwright(rps_no: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        start_time, reaching_time = None, None

        try:
            page.goto("http://smart.dsmsoft.com/FMSSmartApp/Safex_RPS_Reports/RPS_Reports.aspx?usergroup=NRM.101", wait_until="domcontentloaded", timeout=120000)

            page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[2]').click()
            page.wait_for_timeout(500)
            page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[3]/div[2]/ul/li[1]/input').click()
            page.wait_for_timeout(500)
            page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[1]/div[2]/input').click()
            page.wait_for_timeout(500)
            page.locator('//div[contains(@class,"xdsoft_datepicker")]//button[contains(@class,"xdsoft_prev")]').nth(0).click()
            page.wait_for_timeout(1000)

            today = datetime.now()
            day_xpath = f'//td[@data-date="{today.day}" and contains(@class, "xdsoft_date") and not(contains(@class, "xdsoft_disabled"))]'
            page.locator(day_xpath).nth(0).click()
            page.wait_for_timeout(500)

            page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[5]/div/button').click()
            page.wait_for_timeout(3000)

            rps_input_xpath = '/html/body/form/div[5]/div/div/div/div/div/div/div[4]/div/table/div/div[4]/div/div/div[3]/div[3]/div/div/div/div[1]/input'
            page.locator(f'xpath={rps_input_xpath}').click()
            page.wait_for_timeout(500)
            page.fill(f'xpath={rps_input_xpath}', rps_no)
            page.wait_for_timeout(2000)

            reaching_xpath = '/html/body/form/div[5]/div/div/div/div/div/div/div[4]/div/table/div/div[6]/div/div/div[1]/div/table/tbody/tr[1]/td[4]'
            start_xpath = '/html/body/form/div[5]/div/div/div/div/div/div/div[4]/div/table/div/div[6]/div/div/div[1]/div/table/tbody/tr[1]/td[3]'

            reaching_time = page.locator(f'xpath={reaching_xpath}').text_content(timeout=5000)
            start_time = page.locator(f'xpath={start_xpath}').text_content(timeout=5000)

        except Exception as e:
            logging.error(f"Error fetching times for RPS {rps_no}: {e}")
        finally:
            browser.close()

        return start_time, reaching_time

def update_and_migrate():
    sheet1, sheet2 = get_sheets()
    headers1 = sheet1.row_values(1)
    headers2 = sheet2.row_values(1)

    start_idx = get_column_index(headers1, "Route_Start_Date_Time")
    reach_idx = get_column_index(headers1, "Actual_Closing_Time")

    col_rps_1 = get_column_index(headers1, "RPS No")
    col_vehicle_1 = get_column_index(headers1, "Vehicle Number")

    col_start_2 = get_column_index(headers2, "Route_Start_Date_Time")
    col_vehicle_2 = get_column_index(headers2, "Vehicle_Number")
    col_rps_2 = get_column_index(headers2, "RPS No")
    col_reach_2 = get_column_index(headers2, "Route_Reaching_Date_Time")

    records = sheet1.get_all_records()
    sheet2_records = sheet2.get_all_records()
    existing_rps_set = {str(row["RPS No"]).strip(): idx+2 for idx, row in enumerate(sheet2_records)}

    for i in range(len(records), 0, -1):
        row = records[i - 1]
        rps_no = str(row.get("RPS No", "")).strip()
        if not rps_no or row.get("Actual_Closing_Time"):
            continue

        vehicle_no = str(row.get("Vehicle Number", "")).strip()
        logging.info(f"Fetching times for RPS No: {rps_no}")
        start_time, reaching_time = fetch_times_with_playwright(rps_no)

        if reaching_time and reaching_time.strip():
            if rps_no not in existing_rps_set:
                new_row = [''] * max(col_start_2, col_vehicle_2, col_rps_2, col_reach_2)
                new_row[col_start_2 - 1] = start_time
                new_row[col_vehicle_2 - 1] = vehicle_no
                new_row[col_rps_2 - 1] = rps_no
                new_row[col_reach_2 - 1] = reaching_time
                sheet2.append_row(new_row)
                logging.info(f"Inserted new RPS {rps_no} in RPS_Closed")
                sheet1.delete_rows(i + 1)
                logging.info(f"Deleted RPS {rps_no} from Sheet3")
            else:
                logging.info(f"Skipped RPS {rps_no} as it already exists in RPS_Closed")
        elif start_time:
            sheet1.update_cell(i + 1, start_idx, start_time)
            logging.info(f"Updated Route_Start_Date_Time for RPS {rps_no}")

        time.sleep(3)

if __name__ == "__main__":
    update_and_migrate()
