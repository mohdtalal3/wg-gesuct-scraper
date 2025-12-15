import requests
import json
import os
from datetime import datetime
from supabase import Client
from logger_config import setup_logger

# Setup logger
logger = setup_logger('wg_scraper')


class WgGesuchtClient:
    """WG-Gesucht API Client with login, session management, and offer scraping."""
    
    API_URL = 'https://www.wg-gesucht.de/api/{}'
    APP_VERSION = '1.28.0'
    CLIENT_ID = 'wg_mobile_app'
    USER_AGENT = (
        'Mozilla/5.0 (Linux; Android 6.0; Google Build/MRA58K; wv) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 '
        'Chrome/74.0.3729.186 Mobile Safari/537.36'
    )
    BASE_URL = "https://www.wg-gesucht.de"

    def __init__(self, proxy_url: str = None):
        self.userId = None
        self.accessToken = None
        self.refreshToken = None
        self.devRefNo = None
        
        # Setup proxy if provided
        self.proxies = None
        if proxy_url:
            self.proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            logger.info(f"🔒 Proxy configured: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")

    # ---------------------------------------------------
    # Generic API request
    # ---------------------------------------------------
    def request(self, method, endpoint, params=None, payload=None):
        url = self.API_URL.format(endpoint)
        headers = {
            'X-App-Version': self.APP_VERSION,
            'User-Agent': self.USER_AGENT,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Client-Id': self.CLIENT_ID,
        }

        if self.accessToken:
            headers['X-Authorization'] = f'Bearer {self.accessToken}'
            headers['X-User-Id'] = self.userId
            headers['X-Dev-Ref-No'] = self.devRefNo

        response = requests.request(method, url, headers=headers, params=params, data=payload, proxies=self.proxies)

        if response.status_code in range(200, 300):
            return response
        
        # if response.status_code == 401:
        #     logger.error(f"❌ Session expired (401) for endpoint {endpoint}. Re-login required.")
        #     return None

        logger.error(f"❌ Request failed for {method} {endpoint}: {response.status_code} — {response.text[:200]}")
        return None

    # ---------------------------------------------------
    # Login
    # ---------------------------------------------------
    def login(self, username: str, password: str, otp: str = None):
        """Login and set session tokens. Returns True on success, False on failure, 'MFA_REQUIRED' if 2FA needed."""
        payload = {
            'login_email_username': username,
            'login_password': password,
            'client_id': self.CLIENT_ID,
            'display_language': 'de'
        }

        r = self.request('POST', 'sessions', None, json.dumps(payload))
        if not r:
            logger.error("❌ Login failed.")
            return False

        response_data = r.json()
        
        # Check if 2FA is required (status 202)
        if response_data.get('status') == 202:
            logger.error("🔐 Two-Factor Authentication required.")
            return 'MFA_REQUIRED'
        
        # Normal login response (status 200)
        body = response_data['detail']
        self.accessToken = body['access_token']
        self.refreshToken = body['refresh_token']
        self.userId = body['user_id']
        self.devRefNo = body['dev_ref_no']
        logger.info("✅ Logged in successfully.")
        return True

    # ---------------------------------------------------
    # Verify 2FA
    # ---------------------------------------------------
    def verify_2fa(self, token: str, verification_code: str):
        """
        Verifies 2FA code and completes login.
        """
        payload = {
            'token': token,
            'verification_code': verification_code
        }

        r = self.request('POST', 'sessions/auth-verifications', None, json.dumps(payload))

        if not r:
            logger.error("❌ 2FA verification failed.")
            return False

        body = r.json()['detail']
        self.accessToken = body['access_token']
        self.refreshToken = body['refresh_token']
        self.userId = body['user_id']
        self.devRefNo = body['dev_ref_no']
        logger.info("✅ 2FA verified successfully.")
        return True

    # ---------------------------------------------------
    # Refresh Token
    # ---------------------------------------------------
    def refresh_session(self):
        """Refresh the access token using refresh token."""
        if not all([self.userId, self.refreshToken, self.devRefNo]):
            logger.warning("⚠️ Missing session data, cannot refresh token.")
            return False

        payload = {
            'grant_type': 'refresh_token',
            'access_token': self.accessToken,
            'refresh_token': self.refreshToken,
            'client_id': self.CLIENT_ID,
            'dev_ref_no': self.devRefNo,
            'display_language': 'de'
        }

        endpoint = f"sessions/users/{self.userId}"
        url = self.API_URL.format(endpoint)
        headers = {
            'X-App-Version': self.APP_VERSION,
            'User-Agent': self.USER_AGENT,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Client-Id': self.CLIENT_ID,
            'X-Authorization': f'Bearer {self.accessToken}',
            'X-User-Id': self.userId,
            'X-Dev-Ref-No': self.devRefNo
        }
        
        response = requests.put(url, headers=headers, data=json.dumps(payload), proxies=self.proxies)
        
        if response.status_code not in range(200, 300):
            logger.error(f"❌ Token refresh failed: {response.status_code} — {response.text}")
            return False

        body = response.json()['detail']
        self.accessToken = body['access_token']
        self.refreshToken = body['refresh_token']
        self.devRefNo = body['dev_ref_no']
        logger.info("🔄 Token refreshed successfully.")
        return True

    # ---------------------------------------------------
    # Get session details as dict
    # ---------------------------------------------------
    def get_session_dict(self):
        """Return session details as a dictionary for storage with timestamp."""
        return {
            'userId': self.userId,
            'accessToken': self.accessToken,
            'refreshToken': self.refreshToken,
            'devRefNo': self.devRefNo,
            'session_created_at': datetime.now().isoformat()
        }

    def set_session_from_dict(self, session_data):
        """Load session from a dictionary."""
        self.userId = session_data.get('userId')
        self.accessToken = session_data.get('accessToken')
        self.refreshToken = session_data.get('refreshToken')
        self.devRefNo = session_data.get('devRefNo')

    # ---------------------------------------------------
    # My Profile
    # ---------------------------------------------------
    def my_profile(self):
        """Get logged-in user's profile."""
        if not self.userId:
            logger.warning("⚠️ Not logged in.")
            return None

        endpoint = f'public/users/{self.userId}'
        r = self.request('GET', endpoint)
        if not r:
            return None
        return r.json()

    # ---------------------------------------------------
    # Fetch all offers (LOGIN REQUIRED - Uses authenticated session)
    # ---------------------------------------------------
    def offers_all(self, cityId: str, categories: list = None, rent_types: list = None, page: str = '1', 
                   exclude_contacted: bool = True, max_rent: int = None, min_size: int = None):
        """
        Fetch offers from WG-Gesucht using authenticated session.
        
        Args:
            cityId: City ID to search in
            categories: List of categories to include (0=WG, 1=1-Room, 2=Apartment, 3=House)
            rent_types: List of rent types to include (1=temporary, 2=indefinite, 3=overnight stay)
            page: Page number
            exclude_contacted: If True, excludes already contacted ads (requires login)
            max_rent: Maximum rent in euros (optional, max: 9999)
            min_size: Minimum size in square meters (optional, max: 999)
        
        Returns:
            JSON response with offers
        """
        if categories is None:
            categories = [0, 1, 2, 3]
        if rent_types is None:
            rent_types = [1, 2]  # Default: temporary and indefinite
            
        categories_str = ','.join(map(str, categories))
        rent_types_str = ','.join(map(str, rent_types))
        
        params = {
            'ad_type': '0',
            'categories': categories_str,
            'rent_types': rent_types_str,
            'city_id': cityId,
            'noDeact': '1',
            'img': '1',
            'limit': '50',
            'page': page
        }
        
        # Add filter to exclude already contacted ads
        if exclude_contacted:
            params['exContAds'] = '1'
        
        # Add optional filters
        if max_rent is not None and max_rent > 0:
            params['rMax'] = str(min(max_rent, 9999))  # Cap at 9999
        
        if min_size is not None and min_size > 0:
            params['sMin'] = str(min(min_size, 999))  # Cap at 999
        
        # Use authenticated request (uses self.request which includes auth headers)
        endpoint = 'asset/offers/'
        r = self.request('GET', endpoint, params=params)
        
        if not r:
            logger.error(f"❌ Failed to fetch offers. Make sure you're logged in.")
            return None
        
        return r.json()

    # ---------------------------------------------------
    # Contact an Offer
    # ---------------------------------------------------
    def contact_offer(self, offerId: str, message: str):
        """Send a message to a WG-Gesucht offer."""
        payload = {
            'user_id': self.userId,
            'ad_type': 0,
            'ad_id': int(offerId),
            'messages': [
                {
                    'content': message,
                    'message_type': 'text'
                }
            ]
        }

        r = self.request('POST', 'conversations', None, json.dumps(payload))
        if not r:
            logger.error(f"❌ Failed to contact offer {offerId} - Request failed (possibly 401/auth issue)")
            return False

        return True


# ===================================================
# UTILITY FUNCTIONS
# ===================================================

def parse_date(date_str):
    """
    Convert WG-Gesucht date string to datetime object.
    Format: "22.10.2025, 17:15:01" (DD.MM.YYYY, HH:MM:SS)
    """
    try:
        return datetime.strptime(date_str, "%d.%m.%Y, %H:%M:%S")
    except Exception:
        return None


def ensure_valid_session(client: WgGesuchtClient, account: dict, supabase: Client) -> bool:
    """
    Ensures the client has a valid session for authenticated requests.
    Checks session age - if older than 40 minutes, proactively refreshes using refresh token.
    
    NOTE: Does NOT auto-login on first run - session must be created by frontend.
    Only uses login function when refresh token fails and re-login is needed.
    
    Returns True if session is valid/refreshed, False if no session or login failed.
    """
    session_details = account.get('session_details')
    
    # No session at all - skip (session must be created from frontend)
    if not session_details:
        logger.warning(f"⚠️ [{account['email']}] No session found. Session must be created from frontend first.")
        return False
    
    # Load existing session
    client.set_session_from_dict(session_details)
    
    # Check session age (sessions valid for 60 min, refresh at 40 min)
    session_created_str = session_details.get('session_created_at')
    
    if session_created_str:
        try:
            session_created = datetime.fromisoformat(session_created_str)
            age_minutes = (datetime.now() - session_created).total_seconds() / 60
            
            logger.info(f"🕐 [{account['email']}] Session age: {age_minutes:.1f} minutes")
            
            # If session is older than 40 minutes, refresh it proactively
            if age_minutes > 40:
                logger.warning(f"⚠️ [{account['email']}] Session older than 40 minutes. Refreshing token...")
                
                if not client.refresh_session():
                    logger.error(f"❌ [{account['email']}] Token refresh failed. Trying full re-login...")
                    if not client.login(account['email'], account['password']):
                        logger.error(f"❌ [{account['email']}] Re-login also failed.")
                        return False
                
                # Update session in database
                new_session = client.get_session_dict()
                supabase.table('accounts').update({
                    'session_details': new_session
                }).eq('id', account['id']).execute()
                logger.info(f"✅ [{account['email']}] Session refreshed and updated.")
                return True
            else:
                logger.info(f"✅ [{account['email']}] Session is fresh (expires in ~{60 - age_minutes:.0f} minutes).")
                return True
                
        except Exception as e:
            logger.warning(f"⚠️ [{account['email']}] Could not parse session timestamp: {e}")
    else:
        logger.warning(f"⚠️ [{account['email']}] No session timestamp found.")
    
    # Validate session with a simple profile check (as fallback)
    logger.info(f"🔄 [{account['email']}] Validating existing session...")
    if client.my_profile():
        logger.info(f"✅ [{account['email']}] Existing session is valid.")
        return True
    
    # Session invalid - try refresh token first, then full login
    logger.warning(f"⚠️ [{account['email']}] Session invalid. Attempting token refresh...")
    if client.refresh_session():
        new_session = client.get_session_dict()
        supabase.table('accounts').update({
            'session_details': new_session
        }).eq('id', account['id']).execute()
        logger.info(f"✅ [{account['email']}] Token refreshed and session updated.")
        return True
    
    # Refresh failed, try full re-login
    logger.warning(f"⚠️ [{account['email']}] Token refresh failed. Re-logging in...")
    login_result = client.login(account['email'], account['password'])
    
    # Check if 2FA is required - auto-disable account
    if login_result == 'MFA_REQUIRED':
        logger.error(f"❌ [{account['email']}] 2FA required during auto-login. Auto-disabling account.")
        config = account.get('configuration', {})
        config['scrape_enabled'] = False
        supabase.table('accounts').update({
            'configuration': config
        }).eq('id', account['id']).execute()
        logger.error(f"🔴 [{account['email']}] Account disabled (scrape_enabled=false). Please re-login from frontend with 2FA.")
        return False
    
    if not login_result:
        logger.error(f"❌ [{account['email']}] Re-login failed.")
        return False
    
    # Update session in database
    new_session = client.get_session_dict()
    supabase.table('accounts').update({
        'session_details': new_session
    }).eq('id', account['id']).execute()
    logger.info(f"✅ [{account['email']}] Re-logged in and session updated.")
    return True


# ===================================================
# MAIN SCRAPER FUNCTION
# ===================================================

def run_scraper_for_account(account: dict, supabase: Client):
    """
    Run scraper for a single account from Supabase.
    
    - Logs in and scrapes listings (excludes already contacted ads via exContAds filter)
    - Filters ONLY offers newer than 'last_latest'
    - FULLY REPLACES listing_data with new filtered offers
    - Updates 'last_latest' to newest timestamp
    - AUTO-CONTACTS new offers if 'message' field is set
    - Updates 'last_updated_at' timestamp
    
    Returns: (success: bool, new_offers_count: int)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"🏃 Running scraper for: {account['email']}")
    logger.info(f"{'='*60}")
    
    # Get configuration from account
    config = account.get('configuration', {})
    city_id = config.get('city_id')
    categories = config.get('categories')
    proxy_port = config.get('proxy_port')
    max_rent = config.get('max_rent')  # Optional
    min_size = config.get('min_size')  # Optional
    
    # Build proxy URL if proxy_port is provided
    proxy_url = None
    if proxy_port:
        proxy_base = os.getenv('PROXY_URL')
        if proxy_base:
            proxy_url = f"{proxy_base}{proxy_port}"
            logger.info(f"🔒 [{account['email']}] Using proxy port: {proxy_port}")
        else:
            logger.warning(f"⚠️ [{account['email']}] PROXY_URL not found in environment, running without proxy")
    else:
        logger.info(f"ℹ️ [{account['email']}] No proxy port configured, running without proxy")
    
    # Initialize client with proxy
    client = WgGesuchtClient(proxy_url=proxy_url)
    
    # Ensure valid session before scraping (required for exContAds filter)
    logger.info(f"🔐 [{account['email']}] Logging in to access filtered listings...")
    if not ensure_valid_session(client, account, supabase):
        logger.error(f"❌ [{account['email']}] Could not establish valid session. Cannot fetch listings.")
        return False, 0
    
    logger.info(f"🔍 [{account['email']}] Fetching offers from city_id={city_id}, categories={categories}...")
    logger.info(f"🚫 [{account['email']}] Excluding already contacted ads (exContAds=1)")
    if max_rent:
        logger.info(f"💰 [{account['email']}] Max rent filter: {max_rent}€")
    if min_size:
        logger.info(f"📏 [{account['email']}] Min size filter: {min_size}m²")
    
    # Get rent_types from configuration (default: [1, 2] = temporary, indefinite)
    rent_types = config.get('rent_types')
    if rent_types:
        logger.info(f"🏠 [{account['email']}] Rent types filter: {rent_types}")
    
    # Fetch offers (authenticated API with exContAds filter)
    raw_response = client.offers_all(
        cityId=city_id, 
        categories=categories,
        rent_types=rent_types,
        exclude_contacted=True,
        max_rent=max_rent,
        min_size=min_size
    )
    if not raw_response:
        logger.error(f"❌ [{account['email']}] No offers found or request failed.")
        return False, 0
    
    # Extract offers array from response
    offers = raw_response.get('_embedded', {}).get('offers', [])
    logger.info(f"✅ [{account['email']}] Fetched {len(offers)} offers.")
    
    # Load existing listing_data to get previous last_latest
    existing_listing_data = account.get('listing_data', {}) or {}
    last_latest_str = existing_listing_data.get('last_latest')
    last_latest_time = parse_date(last_latest_str) if last_latest_str else None
    
    # Extract latest timestamp from fetched offers
    all_times = [
        parse_date(o.get('date_of_entry_details'))
        for o in offers
        if o.get('date_of_entry_details')
    ]
    latest_time_in_fetch = max([t for t in all_times if t], default=None)
    
    # First run: initialize
    if not last_latest_time:
        # Format: "22.10.2025, 17:15:01" - EXACT format from WG-Gesucht API
        latest_str = (
            latest_time_in_fetch.strftime("%d.%m.%Y, %H:%M:%S")
            if latest_time_in_fetch
            else None
        )
        new_listing_data = {
            "last_latest": latest_str,
            "offers": []
        }
        
        supabase.table('accounts').update({
            'listing_data': new_listing_data,
            'last_updated_at': datetime.now().isoformat()
        }).eq('id', account['id']).execute()
        
        logger.info(f"🆕 [{account['email']}] Initialized listing_data with last_latest: {latest_str}")
        logger.info(f"    Next run will save only newer listings.")
        return True, 0
    
    # Subsequent runs: filter only listings newer than last_latest
    logger.info(f"📌 [{account['email']}] Previous last_latest: {last_latest_str}")
    
    new_offers = []
    for o in offers:
        date_str = o.get('date_of_entry_details')
        offer_time = parse_date(date_str)
        if not offer_time:
            continue
        
        if offer_time > last_latest_time:
            formatted = {
                "offer_id": o.get('offer_id'),
                "title": o.get('offer_title'),
                "user_id": o.get('user_id'),
                "public_name": o.get('user_data', {}).get('public_name'),
                "date_of_entry_details": date_str,
                "url": f"{WgGesuchtClient.BASE_URL}/{o.get('offer_id')}.html"
            }
            new_offers.append(formatted)
    
    if not new_offers:
        logger.info(f"✅ [{account['email']}] No new listings found — everything is up to date.")
        # Still update last_updated_at
        supabase.table('accounts').update({
            'last_updated_at': datetime.now().isoformat()
        }).eq('id', account['id']).execute()
        return True, 0
    
    # Update "last_latest" to the newest time found in new offers
    newest_time = max(parse_date(o['date_of_entry_details']) for o in new_offers)
    # Format: "22.10.2025, 17:15:01" - EXACT format from WG-Gesucht API
    newest_str = newest_time.strftime("%d.%m.%Y, %H:%M:%S")
    
    # Sort new offers by date descending
    new_offers = sorted(
        new_offers,
        key=lambda x: parse_date(x['date_of_entry_details']) or datetime.min,
        reverse=True
    )
    
    # FULLY REPLACE listing_data with ONLY new filtered listings
    updated_listing_data = {
        "last_latest": newest_str,
        "offers": new_offers
    }
    
    # Save to Supabase first
    try:
        supabase.table('accounts').update({
            'listing_data': updated_listing_data,
            'last_updated_at': datetime.now().isoformat()
        }).eq('id', account['id']).execute()
        
        logger.info(f"🆕 [{account['email']}] Added {len(new_offers)} new offers.")
        logger.info(f"📅 [{account['email']}] Updated last_latest → {newest_str}")
        
    except Exception as e:
        logger.error(f"❌ [{account['email']}] Error saving to Supabase: {e}")
        return False, 0
    
    # ===================================================
    # AUTO-CONTACT NEW OFFERS
    # ===================================================
    
    contact_message = account.get('message')
    
    if not contact_message or not contact_message.strip():
        logger.warning(f"⚠️ [{account['email']}] No message found. Skipping auto-contact.")
        return True, len(new_offers)
    
    # Ensure valid session (auto-login if expired)
    logger.info(f"💬 [{account['email']}] Auto-contacting {len(new_offers)} new offers...")
    
    if not ensure_valid_session(client, account, supabase):
        logger.error(f"❌ [{account['email']}] Could not establish valid session. Skipping auto-contact.")
        return True, len(new_offers)
    
    # Contact each offer
    contacted_count = 0
    failed_count = 0
    
    for offer in new_offers:
        offer_id = offer.get('offer_id')
        offer_title = offer.get('title', 'Unknown')
        offer_url = offer.get('url', '')
        
        logger.info(f"📤 [{account['email']}] Contacting offer {offer_id}: {offer_title[:40]}...")
        logger.info(f"   🔗 URL: {offer_url}")
        
        result = client.contact_offer(offer_id, contact_message)
        
        if result:
            contacted_count += 1
            logger.info(f"   ✅ [{account['email']}] Successfully contacted offer {offer_id}")
        else:
            failed_count += 1
            logger.error(f"   ❌ [{account['email']}] Failed to contact offer {offer_id} - Check logs above for details")
    
    # Update the contacted_ads counter in configuration
    if contacted_count > 0:
        try:
            config = account.get('configuration', {})
            current_contacted = config.get('contacted_ads', 0)
            new_total = current_contacted + contacted_count
            
            config['contacted_ads'] = new_total
            
            supabase.table('accounts').update({
                'configuration': config
            }).eq('id', account['id']).execute()
            
            logger.info(f"📈 [{account['email']}] Updated contacted_ads: {current_contacted} → {new_total}")
        except Exception as e:
            logger.error(f"❌ [{account['email']}] Error updating contacted_ads counter: {e}")
    
    logger.info(f"📊 [{account['email']}] Contact Summary: ✅ {contacted_count} | ❌ {failed_count}")
    logger.info(f"✅ [{account['email']}] Scraper completed successfully!")
    
    return True, len(new_offers)

