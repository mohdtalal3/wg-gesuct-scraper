# WG-Gesucht Scraper Backend

A Flask-based backend service that automatically scrapes WG-Gesucht listings and contacts new offers using multiple accounts concurrently.

## Features

- ✅ Automatic scraping every 5 minutes per account
- ✅ Concurrent processing (up to 10 accounts at once)
- ✅ Background thread checks Supabase every 2 minutes
- ✅ Automatic session management (login/refresh)
- ✅ Auto-contact new listings
- ✅ REST API for monitoring and manual triggers
- ✅ Optimized single Supabase client instance (thread-safe, reusable)
- ✅ **Proxy support** - Each account can use its own proxy port

## Setup

### 1. Install Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the `backend/` directory:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
PROXY_URL=http://username:password@proxy-host.com:
```

**Note:** The `PROXY_URL` should end with `:` (colon) as the port will be appended from each account's configuration.

### 3. Setup Supabase Database

Run the SQL script to create the accounts table:

```sql
-- See ../script.sql
```

### 4. Add Accounts to Supabase

Insert accounts directly in Supabase or use the original `wg_scraper.py` script:

```python
from wg_scraper import populate_account_info
populate_account_info()
```

## Running the Backend

```bash
python app.py
```

The server will start on `http://0.0.0.0:5000`

## API Endpoints

### Health Check
```bash
GET /
```

Returns service status.

### Get Statistics
```bash
GET /stats
```

Returns scraper statistics including:
- Total runs
- Successful/failed runs
- Total new offers found
- Currently running scrapers
- Recent account processing history

### List All Accounts
```bash
GET /accounts
```

Returns all WG-Gesucht accounts from Supabase.

### List Ready Accounts
```bash
GET /accounts/ready
```

Returns accounts that are ready to be scraped (haven't been updated in 5+ minutes).

### Manually Trigger Scrape
```bash
POST /scrape/trigger
```

Manually triggers a scrape for all ready accounts (useful for testing).

## How It Works

### Background Thread
- Runs continuously in the background
- Checks Supabase every **2 minutes**
- Finds accounts where `last_updated_at` is older than **5 minutes**
- Processes up to **10 accounts concurrently** using thread pool

### Scraping Process (Per Account)
1. Fetch public listings from WG-Gesucht API (no auth needed)
2. Filter new listings (newer than `last_latest` timestamp)
3. Save new listings to `listing_data` in Supabase
4. Update `last_latest` timestamp
5. Auto-contact new listings if `message` field is set
6. Update `last_updated_at` timestamp

### Session Management
- Sessions must be created from frontend when adding accounts
- Backend loads existing sessions from Supabase
- Sessions are valid for 60 minutes
- Automatically refreshes token at 40 minutes
- Falls back to re-login if refresh fails (only time login is called)
- No auto-login on first run - accounts without sessions will skip auto-contact

## Configuration

### Backend Settings

Edit these constants in `app.py`:

```python
SCRAPER_INTERVAL = 5  # minutes - how often to scrape per account
QUEUE_CHECK_INTERVAL = 2  # minutes - how often to check for ready accounts
MAX_CONCURRENT_SCRAPERS = 10  # max concurrent account processing
```

### Proxy Configuration

Each account can use its own proxy. The proxy system works as follows:

**1. Setup PROXY_URL in .env:**
```env
PROXY_URL=http://username:password@proxy-host.com:
```
Note: End with `:` (colon) - the port will be appended automatically.

**2. Add proxy_port to account configuration:**
```json
{
  "configuration": {
    "city_id": "8",
    "categories": [0, 1, 2, 3],
    "proxy_port": 15872
  }
}
```

**3. The backend automatically combines them:**
```
Full Proxy URL = PROXY_URL + proxy_port
Example: http://username:password@proxy-host.com:15872
```

**Benefits:**
- Each account can use a different proxy port
- Proxies are optional - accounts without `proxy_port` run without proxy
- All requests (scraping, login, contact) use the configured proxy
- Proxy configuration is per-account, allowing flexible setups

## Account Configuration in Supabase

Each account should have:

```json
{
  "email": "your-email@example.com",
  "password": "your-password",
  "website": "wg-gesucht",
  "configuration": {
    "city_id": "8",  // Berlin
    "categories": [0, 1, 2, 3],  // All types
    "proxy_port": 15872  // Optional: Proxy port for this account
  },
  "message": "Your contact message here...",
  "session_details": {
    "userId": "123",
    "accessToken": "...",
    "refreshToken": "...",
    "devRefNo": "...",
    "session_created_at": "2025-10-22T14:30:00"
  }
}
```

**Important Notes:**
- `session_details` must be created by your frontend when adding accounts. The backend will NOT auto-login; it only uses existing sessions.
- `proxy_port` is optional. If provided, it will be combined with `PROXY_URL` from `.env` to create the full proxy URL for this account.

## Monitoring

Check real-time statistics:

```bash
curl http://localhost:5000/stats
```

Example response:
```json
{
  "stats": {
    "total_runs": 45,
    "successful_runs": 43,
    "failed_runs": 2,
    "total_new_offers": 127,
    "currently_running": 3,
    "last_check": "2025-10-22T14:30:00",
    "accounts_processed": [...]
  },
  "config": {
    "scraper_interval_minutes": 5,
    "queue_check_interval_minutes": 2,
    "max_concurrent_scrapers": 10
  }
}
```

## Production Deployment

For production, use a WSGI server like Gunicorn:

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

Or use Docker:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "app.py"]
```

## Troubleshooting

### "No accounts ready to scrape"
- Check that accounts exist in Supabase with `website='wg-gesucht'`
- Verify `last_updated_at` is older than 5 minutes
- Check logs for errors

### "Login failed"
- Verify credentials in Supabase are correct
- Check if WG-Gesucht account is active
- Review session_details in database

### "Failed to contact offer"
- Ensure `message` field is set and not empty
- Check session is valid
- Verify offer_id is correct

## License

MIT

