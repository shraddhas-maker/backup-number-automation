# Backup PN automation

## Setup
1. Create virtualenv:
   python3 -m venv venv
   source venv/bin/activate

2. Install:
   pip install -r requirements.txt

3. Edit config.py:
   - DB credentials
   - SMTP credentials
   - ADD_PN_API URL and headers
   - CSV file paths or Google Sheets creds

4. Prepare CSVs:
   data/accounts.csv (tenant, account_id, status, email)
   data/tenant_exceptions.csv (tenant, allowed_operators)
   data/region_preferences.csv (region, pilots)

5. Run once:
   python run_backup.py

6. Schedule with cron (example below)


7. Cron command
*/5 * * * * cd /path/to/backup-system && /path/to/venv/bin/python run_backup.py >> /path/to/backup-system/logs/cron.log 2>&1



source venv/bin/activate

# 1. Check which files are changed
git status

# 2. Add ALL updated files
git add .

# 3. Commit your changes
git commit -m "Updated Truecaller scraper and dashboard code"

# 4. Push to GitHub
git push


# 5. Pull data 
git pull
