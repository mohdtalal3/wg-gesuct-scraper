import os
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, jsonify
from dotenv import load_dotenv
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor, as_completed
from wg_scraper import run_scraper_for_account

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Configuration
SCRAPER_INTERVAL = 1  # minutes - how often to scrape per account
QUEUE_CHECK_INTERVAL = 1  # minutes - how often to check for accounts ready to scrape
MAX_CONCURRENT_SCRAPERS = 10  # max number of accounts to scrape concurrently

app = Flask(__name__)

# Initialize global Supabase client (thread-safe, reusable)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global state
scraper_stats = {
    'total_runs': 0,
    'successful_runs': 0,
    'failed_runs': 0,
    'total_new_offers': 0,
    'last_check': None,
    'currently_running': 0,
    'accounts_processed': []
}


def get_accounts_ready_to_scrape(supabase: Client):
    """
    Fetch accounts from Supabase that are:
    1. Website = 'wg-gesucht'
    2. Haven't been updated in the last SCRAPER_INTERVAL minutes
    
    Returns list of account dictionaries.
    """
    try:
        # Fetch all wg-gesucht accounts
        response = supabase.table('accounts').select('*').eq('website', 'wg-gesucht').execute()
        
        if not response.data:
            return []
        
        ready_accounts = []
        now = datetime.now()
        
        for account in response.data:
            # Check if account has been updated recently
            last_updated_str = account.get('last_updated_at')
            
            if not last_updated_str:
                # Never updated - ready to scrape
                ready_accounts.append(account)
                continue
            
            try:
                last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                # Remove timezone info for comparison
                if last_updated.tzinfo is not None:
                    last_updated = last_updated.replace(tzinfo=None)
                
                time_since_update = (now - last_updated).total_seconds() / 60  # minutes
                
                if time_since_update >= SCRAPER_INTERVAL:
                    ready_accounts.append(account)
                    print(f"✅ Account {account['email']} ready (last updated {time_since_update:.1f} min ago)")
                else:
                    print(f"⏳ Account {account['email']} not ready (last updated {time_since_update:.1f} min ago, needs {SCRAPER_INTERVAL - time_since_update:.1f} more min)")
            
            except Exception as e:
                print(f"⚠️ Error parsing timestamp for {account['email']}: {e}")
                # If can't parse, consider it ready
                ready_accounts.append(account)
        
        return ready_accounts
    
    except Exception as e:
        print(f"❌ Error fetching accounts from Supabase: {e}")
        return []


def process_account(account: dict):
    """
    Process a single account - run scraper and update stats.
    Uses global supabase client (thread-safe).
    
    Returns: (account_email, success, new_offers_count)
    """
    try:
        success, new_offers_count = run_scraper_for_account(account, supabase)
        return (account['email'], success, new_offers_count)
    except Exception as e:
        print(f"❌ Error processing account {account['email']}: {e}")
        return (account['email'], False, 0)


def scraper_queue_thread():
    """
    Background thread that checks for accounts ready to scrape every QUEUE_CHECK_INTERVAL minutes.
    Processes up to MAX_CONCURRENT_SCRAPERS accounts concurrently.
    Uses global supabase client (thread-safe).
    """
    print(f"🚀 Scraper queue thread started!")
    print(f"   - Checking every {QUEUE_CHECK_INTERVAL} minutes")
    print(f"   - Scraping accounts every {SCRAPER_INTERVAL} minutes")
    print(f"   - Max concurrent scrapers: {MAX_CONCURRENT_SCRAPERS}")
    
    while True:
        try:
            print(f"\n{'='*60}")
            print(f"🔍 Checking for accounts ready to scrape...")
            print(f"{'='*60}")
            
            scraper_stats['last_check'] = datetime.now().isoformat()
            
            ready_accounts = get_accounts_ready_to_scrape(supabase)
            
            if not ready_accounts:
                print("✅ No accounts ready to scrape at this time.")
            else:
                print(f"📋 Found {len(ready_accounts)} accounts ready to scrape")
                
                # Process accounts concurrently using ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPERS) as executor:
                    # Submit all tasks
                    future_to_account = {
                        executor.submit(process_account, account): account 
                        for account in ready_accounts
                    }
                    
                    scraper_stats['currently_running'] = len(future_to_account)
                    
                    # Process completed tasks
                    for future in as_completed(future_to_account):
                        account = future_to_account[future]
                        try:
                            email, success, new_offers_count = future.result()
                            
                            scraper_stats['total_runs'] += 1
                            
                            if success:
                                scraper_stats['successful_runs'] += 1
                                scraper_stats['total_new_offers'] += new_offers_count
                                scraper_stats['accounts_processed'].append({
                                    'email': email,
                                    'timestamp': datetime.now().isoformat(),
                                    'new_offers': new_offers_count,
                                    'status': 'success'
                                })
                                print(f"✅ Account {email} processed successfully ({new_offers_count} new offers)")
                            else:
                                scraper_stats['failed_runs'] += 1
                                scraper_stats['accounts_processed'].append({
                                    'email': email,
                                    'timestamp': datetime.now().isoformat(),
                                    'new_offers': 0,
                                    'status': 'failed'
                                })
                                print(f"❌ Account {email} processing failed")
                        
                        except Exception as e:
                            scraper_stats['failed_runs'] += 1
                            print(f"❌ Exception processing account {account['email']}: {e}")
                    
                    scraper_stats['currently_running'] = 0
                
                # Keep only last 100 processed accounts in memory
                if len(scraper_stats['accounts_processed']) > 100:
                    scraper_stats['accounts_processed'] = scraper_stats['accounts_processed'][-100:]
                
                print(f"\n📊 Batch Summary:")
                print(f"   Total runs: {scraper_stats['total_runs']}")
                print(f"   Successful: {scraper_stats['successful_runs']}")
                print(f"   Failed: {scraper_stats['failed_runs']}")
                print(f"   Total new offers found: {scraper_stats['total_new_offers']}")
        
        except Exception as e:
            print(f"❌ Error in scraper queue thread: {e}")
        
        # Wait for next check interval
        print(f"\n⏳ Waiting {QUEUE_CHECK_INTERVAL} minutes until next check...")
        time.sleep(QUEUE_CHECK_INTERVAL * 60)


# ===================================================
# FLASK ROUTES
# ===================================================

@app.route('/')
def index():
    """Health check endpoint."""
    return jsonify({
        'status': 'running',
        'service': 'WG-Gesucht Scraper Backend',
        'version': '1.0.0'
    })


@app.route('/stats')
def stats():
    """Get scraper statistics."""
    return jsonify({
        'stats': scraper_stats,
        'config': {
            'scraper_interval_minutes': SCRAPER_INTERVAL,
            'queue_check_interval_minutes': QUEUE_CHECK_INTERVAL,
            'max_concurrent_scrapers': MAX_CONCURRENT_SCRAPERS
        }
    })


@app.route('/accounts')
def accounts():
    """Get all wg-gesucht accounts from Supabase."""
    try:
        response = supabase.table('accounts').select('id, email, website, last_updated_at, configuration').eq('website', 'wg-gesucht').execute()
        
        return jsonify({
            'success': True,
            'count': len(response.data),
            'accounts': response.data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/accounts/ready')
def accounts_ready():
    """Get accounts that are ready to be scraped."""
    try:
        ready_accounts = get_accounts_ready_to_scrape(supabase)
        
        return jsonify({
            'success': True,
            'count': len(ready_accounts),
            'accounts': [
                {
                    'id': acc['id'],
                    'email': acc['email'],
                    'last_updated_at': acc.get('last_updated_at')
                }
                for acc in ready_accounts
            ]
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/scrape/trigger', methods=['POST'])
def trigger_scrape():
    """Manually trigger a scrape check (useful for testing)."""
    try:
        ready_accounts = get_accounts_ready_to_scrape(supabase)
        
        if not ready_accounts:
            return jsonify({
                'success': True,
                'message': 'No accounts ready to scrape',
                'count': 0
            })
        
        # Process accounts in a separate thread to not block the request
        def async_scrape():
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPERS) as executor:
                futures = [executor.submit(process_account, account) for account in ready_accounts]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Error in async scrape: {e}")
        
        thread = threading.Thread(target=async_scrape)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Triggered scraping for {len(ready_accounts)} accounts',
            'count': len(ready_accounts)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ===================================================
# MAIN
# ===================================================

if __name__ == '__main__':
    # Start background scraper thread
    scraper_thread = threading.Thread(target=scraper_queue_thread, daemon=True)
    scraper_thread.start()
    
    print(f"\n{'='*60}")
    print("🚀 WG-GESUCHT SCRAPER BACKEND STARTED")
    print(f"{'='*60}")
    print(f"🌐 Flask server starting on http://0.0.0.0:5000")
    print(f"📋 Available endpoints:")
    print(f"   GET  /           - Health check")
    print(f"   GET  /stats      - Scraper statistics")
    print(f"   GET  /accounts   - List all accounts")
    print(f"   GET  /accounts/ready - List accounts ready to scrape")
    print(f"   POST /scrape/trigger - Manually trigger scrape")
    print(f"{'='*60}\n")
    
    # Start Flask app
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)

