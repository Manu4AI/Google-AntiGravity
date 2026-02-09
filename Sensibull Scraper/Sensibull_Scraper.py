import os
import time
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Constants
URL = "https://web.sensibull.com/options-screener?view=table"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(OUTPUT_DIR, "Sensibull_Data.csv")
TXT_FILE = os.path.join(OUTPUT_DIR, "Sensibull_Table.txt")

def full_page_screenshot(driver, file_path):
    """
    Takes a full page screenshot.
    Currently used for debugging.
    """
    try:
        driver.save_screenshot(file_path)
        print(f"[DEBUG] Screenshot saved to {file_path}")
    except Exception as e:
        print(f"[WARN] Failed to take screenshot: {e}")

def get_screener_data():
    """
    Launches Headless Chrome, navigates to Sensibull Screener,
    extracts the table data, and returns headers and rows.
    """
    print(f"[INFO] Launching Headless Chrome...")
    
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        print(f"[INFO] Navigating to {URL}...")
        driver.get(URL)

        # Wait for table to load
        print("[INFO] Waiting for table data...")
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            time.sleep(5) # Initial load wait
        except Exception as e:
            print(f"[WARN] Timeout waiting for table: {e}")
            return [], []

        # Pagination Handling
        print("[INFO] Handling pagination...")
        
        # 1. Try to expand rows per page first
        try:
            # Look for "Rows per page" dropdown: span "Show" -> div -> button
            rows_dropdown = driver.find_elements(By.XPATH, "//span[contains(text(), 'Show')]/following-sibling::div//button")
            
            if rows_dropdown:
                print("[INFO] Found rows per page dropdown. Attempting to expand...")
                driver.execute_script("arguments[0].click();", rows_dropdown[0])
                time.sleep(1)
                # Look for largest option (e.g. 100)
                options = driver.find_elements(By.XPATH, "//li[contains(text(), '100')] | //div[contains(text(), '100')]")
                if options:
                    driver.execute_script("arguments[0].click();", options[-1])
                    print("[INFO] Selected '100' rows.")
                    time.sleep(5) # Wait for reload
        except Exception as e:
            print(f"[WARN] Could not expand rows: {e}")

        # 2. Iterate through pages
        all_rows_data = []
        page_num = 1
        headers = []
        
        while True:
            print(f"[INFO] Scraping Page {page_num}...")
            
            # Extract data with retry for Stale Element
            retry_count = 0
            page_rows = []
            
            while retry_count < 3:
                try:
                    # Find table
                    try:
                        table = driver.find_element(By.TAG_NAME, "table")
                    except:
                        # Sometimes table needs re-finding
                        time.sleep(1)
                        table = driver.find_element(By.TAG_NAME, "table")

                    # Get headers if not yet retrieved
                    if not headers:
                        header_elements = table.find_elements(By.TAG_NAME, "th")
                        if not header_elements:
                            rows_tr = table.find_elements(By.TAG_NAME, "tr")
                            if rows_tr:
                                header_elements = rows_tr[0].find_elements(By.TAG_NAME, "td")
                        headers = [h.text.strip() for h in header_elements if h.text.strip()]
                    
                    # Get rows
                    row_elements = table.find_elements(By.TAG_NAME, "tr")
                    
                    # Skip header row
                    start_index = 0
                    if row_elements and headers and len(row_elements) > 0:
                         first_row_text = row_elements[0].text.strip()
                         if first_row_text and headers and first_row_text.startswith(headers[0].split(' ')[0]):
                             start_index = 1
                    
                    # Collect row data
                    page_rows = []
                    for tr in row_elements[start_index:]:
                        cells = tr.find_elements(By.TAG_NAME, "td")
                        row_data = [cell.text.replace('\n', ' ').strip() for cell in cells]
                        if len(row_data) > 3:
                            page_rows.append(row_data)
                            
                    # If successful, break retry loop
                    break 
                except Exception as e:
                    print(f"[WARN] Stale element or error on page {page_num} (Attempt {retry_count+1}): {e}")
                    retry_count += 1
                    time.sleep(2)
            
            if not page_rows and retry_count == 3:
                print(f"[ERROR] Failed to scrape page {page_num} after retries.")
                break

            print(f"[INFO] Found {len(page_rows)} rows on page {page_num}.")
            all_rows_data.extend(page_rows)

            # Find Next Button
            try:
                # Identified SVG path for Next button: M9 18L15 12L9 6
                # Or look for button after "Page X of Y"
                
                # Check current page info to see if we are at the end
                page_info = driver.find_elements(By.XPATH, "//div[contains(text(), 'Page') and contains(text(), 'of')]")
                if page_info:
                    print(f"[DEBUG] Page Info: {page_info[0].text}") 
                    # Example: Page 1 of 4

                # Robust Next Button Selector: Button following the "Page X of Y" div
                next_buttons = driver.find_elements(By.XPATH, "//div[contains(text(), 'Page')]/following-sibling::button[1]")
                
                # Fallback to SVG path if above fails
                if not next_buttons:
                     next_buttons = driver.find_elements(By.XPATH, "//*[local-name()='svg' and .//*[local-name()='path' and @d='M9 18L15 12L9 6']]/ancestor::button")

                if next_buttons:
                    next_btn = next_buttons[0]
                    
                    # Check if disabled
                    if not next_btn.is_enabled() or "disabled" in next_btn.get_attribute("class"):
                        print("[INFO] Next button disabled. Reached last page.")
                        break
                    
                    # Click
                    driver.execute_script("arguments[0].click();", next_btn)
                    print("[INFO] Clicked Next Page.")
                    page_num += 1
                    time.sleep(5) # Wait for load
                else:
                    print("[WARN] Next button not found via robust selectors. Stopping pagination.")
                    break
                    
            except Exception as e:
                print(f"[WARN] Error navigating pages: {e}")
                break

        # Remove duplicates
        unique_rows = []
        seen = set()
        for row in all_rows_data:
            row_tuple = tuple(row)
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)
        
        print(f"[INFO] Total unique rows extracted: {len(unique_rows)}")
        rows = unique_rows

        # Sort by IVP
        try:
            ivp_index = -1
            for i, h in enumerate(headers):
                if "IVP" in h:
                    ivp_index = i
                    break
            
            if ivp_index != -1:
                print(f"[INFO] Sorting by IVP (Column Index: {ivp_index})...")
                def safe_float(val):
                    try:
                        return float(val.replace('%', '').replace(',', '').strip())
                    except:
                        return -1.0
                rows.sort(key=lambda x: safe_float(x[ivp_index]), reverse=True)
        except Exception as e:
             print(f"[WARN] Error sorting: {e}")

        return headers, rows

    except Exception as e:
        print(f"[ERROR] An error occurred: {e}")
        full_page_screenshot(driver, os.path.join(OUTPUT_DIR, "debug_error.png"))
        return [], []
    finally:
        driver.quit()

def save_to_csv(headers, rows):
    """Saves data to CSV."""
    if not rows:
        print("[WARN] No data to save to CSV.")
        return

    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            # writer = csv.writer(f)
             # Use tab as delimiter if needed, but comma is standard for CSV
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"[SUCCESS] Data saved to {CSV_FILE}")
    except Exception as e:
        print(f"[ERROR] Could not save CSV: {e}")

def save_to_txt(headers, rows):
    """Formats and saves data to a text file (simulating Telegram msg)."""
    if not rows:
        with open(TXT_FILE, "w", encoding="utf-8") as f:
            f.write("No data found.")
        return

    # Basic formatting
    # Try to align columns
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(val))
    
    # Cap width to avoid massive lines
    col_widths = [min(w, 20) for w in col_widths]

    fmt = "  ".join([f"{{:<{w}}}" for w in col_widths])
    
    lines = []
    lines.append(f"Sensibull Options Screener - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 60)
    lines.append(fmt.format(*headers))
    lines.append("-" * 60)
    
    for row in rows:
        # truncate data to fit width
        clean_row = [str(val)[:18] for val in row]
        # Ensure row length matches headers
        if len(clean_row) < len(headers):
            clean_row.extend([''] * (len(headers) - len(clean_row)))
        elif len(clean_row) > len(headers):
            clean_row = clean_row[:len(headers)]
            
        lines.append(fmt.format(*clean_row))

    try:
        with open(TXT_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"[SUCCESS] Table view saved to {TXT_FILE}")
    except Exception as e:
        print(f"[ERROR] Could not save TXT: {e}")

if __name__ == "__main__":
    headers, rows = get_screener_data()
    
    if rows:
        print(f"[INFO] Extracted {len(rows)} rows.")
        save_to_csv(headers, rows)
        save_to_txt(headers, rows)
    else:
        print("[WARN] No rows extracted. Check debug screenshots if available.")
