# BestMed Email Processor

A Flask-based web application that processes Microsoft Outlook `.msg` email files stored in Supabase cloud storage, extracts metadata and attachments, and stores structured data in a Supabase database.

## Features

- 📧 **Email Processing**: Parses `.msg` files and extracts all metadata
- 📎 **Attachment Handling**: Automatically uploads and catalogs email attachments
- 🗄️ **Database Storage**: Stores email metadata in normalized Supabase tables
- 📊 **Real-time Monitoring**: Live log streaming via web interface
- ♻️ **Idempotent Processing**: Automatically skips already-processed emails
- ❌ **Error Tracking**: Logs failed processing attempts to database
- 🔒 **Secure**: Uses environment variables for credentials

## Architecture

```
Supabase Storage (Bucket: "Bestmed")
├── Emails/                    # INPUT: .msg files organized in folders
└── email_attachments/         # OUTPUT: Extracted bodies & attachments
    └── {folder}/{email}/
        ├── {email}.txt        # Email body
        └── {attachment}.*     # Attachments

Supabase Database
├── emails                     # Email metadata & processing status
└── attachments                # Attachment metadata (linked to emails)
```

## Prerequisites

- Python 3.8+
- Supabase account with:
  - Storage bucket created
  - Database tables created (see setup below)

## Setup Instructions

### 1. Clone and Install Dependencies

```bash
cd bestmed
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your Supabase credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_key_here

# Bucket Configuration
BUCKET_NAME=Bestmed
INPUT_FOLDER=Emails
OUTPUT_FOLDER=email_attachments

# Flask Configuration
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
```

**Where to find your Supabase credentials:**
1. Go to your Supabase project dashboard
2. Click **Settings** → **API**
3. Copy the **Project URL** (SUPABASE_URL)
4. Copy the **anon/public** key (SUPABASE_KEY)

### 3. Create Database Tables

Run the SQL migration in your Supabase SQL Editor:

1. Go to your Supabase dashboard
2. Navigate to **SQL Editor**
3. Open [migrations/001_create_email_tables.sql](migrations/001_create_email_tables.sql)
4. Copy the entire contents
5. Paste into Supabase SQL Editor and click **Run**

This creates:
- `emails` table - Email metadata and processing status
- `attachments` table - Attachment metadata
- Indexes for performance
- Helpful views for reporting

### 4. Create Supabase Storage Bucket

1. Go to **Storage** in Supabase dashboard
2. Create a new bucket named `Bestmed` (or your custom name from `.env`)
3. Set permissions as needed (public or private)
4. Create a folder named `Emails` inside the bucket
5. Upload your `.msg` files to the `Emails` folder

### 5. Run the Application

**Local Development:**

```bash
python email_attachment.py
```

Visit `http://localhost:5000` to see the web interface.

**Production (Heroku):**

The app is configured to run on Heroku via the included `Procfile`:

```bash
git push heroku main
```

Make sure to set environment variables in Heroku:

```bash
heroku config:set SUPABASE_URL=your_url
heroku config:set SUPABASE_KEY=your_key
```

## Usage

### Web Interface

1. Navigate to `http://localhost:5000` (or your deployed URL)
2. The processing starts automatically when the page loads
3. Watch real-time logs as emails are processed
4. Processing status updates live in the browser

### API Endpoints

#### `GET /`
Web dashboard with live log streaming

#### `GET /start-processing`
Manually trigger email processing (idempotent - safe to call multiple times)

**Response:**
```json
{
  "message": "✅ Processing started..."
}
```

#### `GET /logs`
Server-Sent Events (SSE) stream for real-time logs

#### `GET /health`
Health check with processing statistics

**Response:**
```json
{
  "status": "ok",
  "processing_started": true,
  "processing_complete": true,
  "emails_total": 150,
  "emails_success": 148,
  "emails_failed": 2,
  "success_rate": 98.67
}
```

## Database Schema

### `emails` Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key (auto-generated) |
| `created_at` | TIMESTAMP | When record was created |
| `folder` | TEXT | Source folder path |
| `file_name` | TEXT | Original .msg filename |
| `file_size_bytes` | BIGINT | Size of .msg file |
| `sender` | TEXT | Email from address |
| `recipients` | TEXT | To field |
| `cc_recipients` | TEXT | CC field |
| `bcc_recipients` | TEXT | BCC field |
| `email_date` | TIMESTAMP | Email sent date |
| `subject` | TEXT | Email subject |
| `body_preview` | TEXT | First 200 chars of body |
| `body_file_path` | TEXT | Path to uploaded body.txt |
| `attachment_count` | INTEGER | Number of attachments |
| `processing_status` | TEXT | 'success', 'failed', or 'partial' |
| `error_message` | TEXT | Error details if failed |
| `processing_duration_ms` | INTEGER | Processing time in milliseconds |
| `processing_started_at` | TIMESTAMP | When processing started |
| `processing_completed_at` | TIMESTAMP | When processing finished |

**Constraints:**
- `UNIQUE(file_name, folder)` - Prevents duplicate processing

### `attachments` Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key (auto-generated) |
| `email_id` | UUID | Foreign key to emails table |
| `created_at` | TIMESTAMP | When record was created |
| `file_name` | TEXT | Attachment filename |
| `file_path` | TEXT | Path in Supabase storage |
| `file_size_bytes` | BIGINT | Size of attachment |
| `file_extension` | TEXT | File extension (e.g., 'pdf', 'docx') |
| `attachment_index` | INTEGER | Order in email |

## Querying the Data

### Example Queries

**Get all emails from a specific sender:**
```sql
SELECT * FROM emails
WHERE sender LIKE '%@example.com'
ORDER BY email_date DESC;
```

**Find emails with failed processing:**
```sql
SELECT file_name, folder, error_message
FROM emails
WHERE processing_status = 'failed';
```

**Get emails with attachments:**
```sql
SELECT
    e.subject,
    e.sender,
    e.attachment_count,
    a.file_name,
    a.file_extension
FROM emails e
JOIN attachments a ON e.id = a.email_id
WHERE e.attachment_count > 0;
```

**Processing summary (use the built-in view):**
```sql
SELECT * FROM email_processing_summary;
```

**Get emails with their attachments as JSON:**
```sql
SELECT * FROM emails_with_attachments
WHERE sender = 'john@example.com';
```

## File Structure

```
bestmed/
├── .env                      # Environment variables (gitignored)
├── .env.example              # Template for environment variables
├── .gitignore               # Git ignore rules
├── README.md                # This file
├── requirements.txt         # Python dependencies
├── Procfile                 # Heroku deployment config
├── email_attachment.py      # Main application
└── migrations/
    └── 001_create_email_tables.sql  # Database schema
```

## Troubleshooting

### "Missing required environment variables"
- Ensure `.env` file exists with valid `SUPABASE_URL` and `SUPABASE_KEY`
- Check that `load_dotenv()` can find the `.env` file

### "Failed to insert email to database"
- Verify database tables were created (run migration SQL)
- Check Supabase API key has database insert permissions
- Review Row Level Security (RLS) policies in Supabase

### "Failed to upload to Supabase storage"
- Ensure bucket exists and matches `BUCKET_NAME` in `.env`
- Check storage permissions in Supabase dashboard
- Verify API key has storage upload permissions

### Emails not processing
- Check that `.msg` files are in the correct folder (`Emails/` by default)
- View logs in web interface for specific errors
- Query `emails` table for records with `processing_status = 'failed'`

## Development

### Adding New Metadata Fields

1. **Update database schema:**
   ```sql
   ALTER TABLE emails ADD COLUMN new_field TEXT;
   ```

2. **Update `process_msg_bytes()` function:**
   ```python
   email_data = {
       ...
       'new_field': msg.new_property,
   }
   ```

3. **Restart the application**

### Running Tests

```bash
# Install test dependencies
pip install pytest

# Run tests (if test suite exists)
pytest
```

## Security Notes

- ✅ `.env` file is gitignored - credentials never committed to git
- ✅ Use `.env.example` as template for other developers
- ✅ Supabase API keys should use least-privilege access
- ⚠️ Consider enabling Row Level Security (RLS) on tables for production
- ⚠️ Review storage bucket permissions based on use case

## License

[Add your license here]

## Support

For issues or questions, please [open an issue](https://github.com/yourorg/bestmed/issues).
