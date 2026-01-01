import os
import time
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pyotp
from breeze_connect import BreezeConnect
import paper_config


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE = os.path.join(BASE_DIR, "session_token.txt")

def check_existing_session():
    """Checks if a valid session token already exists."""
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r") as f:
                token = f.read().strip()
            
            if not token: return None

            print(" [Auto-Login] Found existing session token. Verifying...")
            # Verify by initializing Breeze
            breeze = BreezeConnect(api_key=paper_config.API_KEY)
            breeze.generate_session(api_secret=paper_config.SECRET_KEY, session_token=token)
            
            # Try a lightweight API call (e.g. Get Customer Details or a Quote)
            # If invalid, it usually throws exception or returns Error
            try:
                # Get Quote for NIFTY to test
                data = breeze.get_quotes(stock_code="NIFTY", exchange_code="NSE", product_type="cash", right="others", strike_price="0")
                if 'Success' in data or 'Status' in data: # Assuming success if we get a response
                     print(" [Auto-Login] Existing Session is VALID.")
                     return token
            except:
                print(" [Auto-Login] Existing Session EXPIRED/INVALID.")
                pass
        except Exception as e:
            error_msg = str(e)
            if "Unable to retrieve customer details" in error_msg:
                print(f" [Auto-Login] API Warning: {error_msg}")
                print(" [Auto-Login] Assuming existing session is VALID despite API error.")
                return token
            print(f" [Auto-Login] Error checking session: {e}")
    return None

def get_session_token():
    """
    Automates the login process to get the API Session Token.
    Returns: session_token (str) or None if failed.
    """
    # 1. Check Existing Session
    existing_token = check_existing_session()
    if existing_token:
        return existing_token

    print(" [Auto-Login] Starting Fresh Login Process...")
    
    # Check Credentials
    if paper_config.USER_ID == "YOUR_USER_ID":
        print(" [Auto-Login] ERROR: Please update paper_config.py with your credentials.")
        return None

    # OTP Generation
    try:
        totp = pyotp.TOTP(paper_config.TOTP_KEY)
        current_otp = totp.now()
        print(f" [Auto-Login] Generated TOTP: {current_otp}")
    except Exception as e:
        print(f" [Auto-Login] ERROR generating TOTP. check your TOTP_KEY. {e}")
        return None

    # Browser Setup
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

    driver = None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 1. Login URL
        login_url = f"https://api.icicidirect.com/apiuser/login?api_key={urllib.parse.quote(paper_config.API_KEY)}"
        print(f" [Auto-Login] Navigating to Login Page...")
        driver.get(login_url)

        wait = WebDriverWait(driver, 15)

        # 2. Enter User ID
        print(" [Auto-Login] Entering User ID...")
        # Correct ID from HTML: txtuid
        user_field = wait.until(EC.presence_of_element_located((By.ID, "txtuid")))
        user_field.clear()
        user_field.send_keys(paper_config.USER_ID)
        
        # 3. Enter Password
        print(" [Auto-Login] Entering Password...")
        # Correct ID from HTML: txtPass
        pass_field = driver.find_element(By.ID, "txtPass")
        pass_field.clear()
        pass_field.send_keys(paper_config.PASSWORD)
        
        
        # 3b. Click T&C Checkbox
        print(" [Auto-Login] Clicking T&C Checkbox...")
        try:
             # Using JS click because sometimes checkboxes are intercepted by labels
             tnc_box = driver.find_element(By.ID, "chkssTnc")
             driver.execute_script("arguments[0].click();", tnc_box)
        except Exception as e:
             print(f" [Auto-Login] Checkbox click failed (might be already checked?): {e}")

        # 4. Click Login Button (Step 1)
        print(" [Auto-Login] Clicking Login (Step 1)...")
        # Correct ID: btnSubmit
        login_btn = driver.find_element(By.ID, "btnSubmit")
        login_btn.click()
        
        # 5. Wait for OTP Field (Step 2)
        print(" [Auto-Login] Waiting for OTP Screen...")
        # OTP input usually has class or ID related to otp. 
        # In jscomm.js/HTML, it seems generated or in #dvgetotp. 
        # HTML logic says: input[tg-nm=otp] inside #pnlOTP
        # Let's look for any input type='password' or text visible that is empty?
        # Or check the network logic. Usually field is txtOTP or similar.
        # Based on submitotp() function: input[tg-nm=otp].
        
        otp_inputs = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//input[@tg-nm='otp']")))
        
        if not otp_inputs:
             print(" [Auto-Login] Could not find OTP inputs.")
             return None
             
        # Enter OTP (it might be split into boxes or single box)
        print(" [Auto-Login] Entering OTP...")
        otp_str = str(current_otp)
        
        if len(otp_inputs) > 1:
            # If split boxes
            for i, box in enumerate(otp_inputs):
                if i < len(otp_str):
                    box.send_keys(otp_str[i])
        else:
            # Single box
            otp_inputs[0].send_keys(otp_str)
            
        # 6. Submit OTP
        print(" [Auto-Login] Submitting OTP...")
        try:
            submit_otp_btn = wait.until(EC.element_to_be_clickable((By.ID, "Button1")))
            submit_otp_btn.click()
        except:
             print(" [Auto-Login] Standard click failed, trying JS click...")
             submit_otp_btn = driver.find_element(By.ID, "Button1")
             driver.execute_script("arguments[0].click();", submit_otp_btn)
        
        # 7. Wait for Redirect and Capture Token
        print(" [Auto-Login] Waiting for redirect...")
        wait.until(EC.url_contains("apisession="))
        
        current_url = driver.current_url
        print(f" [Auto-Login] Success URL: {current_url}")
        
        parsed = urllib.parse.urlparse(current_url)
        params = urllib.parse.parse_qs(parsed.query)
        
        if 'apisession' in params:
            session_token = params['apisession'][0]
            print(f" [Auto-Login] SESSION TOKEN CAPTURED: {session_token}")
            
            # SAVE SESSION
            with open(SESSION_FILE, "w") as f:
                f.write(session_token)
            
            return session_token
        else:
            print(" [Auto-Login] ERROR: 'apisession' not found in URL.")
            return None

    except Exception as e:
        print(f" [Auto-Login] FAILED: {str(e)}")
        if driver:
           try:
               driver.save_screenshot("login_error.png")
               with open("login_failed_source.html", "w", encoding="utf-8") as f:
                   f.write(driver.page_source)
               print(" [Auto-Login] Saved debug info.")
           except:
               pass
        return None
        
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    # Test Run
    token = get_session_token()
    if token:
        print("Test Passed!")
    else:
        print("Test Failed.")
