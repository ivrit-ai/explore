# Transcript Index Manager

This application provides functionality to manage and search through transcript data. It supports both building indexes on-the-fly and working with pre-generated flat index files.

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Interface

The application provides a CLI for managing index files:

1. Generate an index file from transcript data:
   ```bash
   python -m app.cli generate-index /path/to/data/dir explore-index.json.gz
   ```

2. Validate an existing index file:
   ```bash
   python -m app.cli validate-index explore-index.json.gz
   ```

### Running the Web Application

You can run the web application in two modes:

1. Building index on-the-fly (default):
   ```bash
   export FLASK_APP=app
   flask run
   ```

2. Using a pre-generated index file:
   ```bash
   export FLASK_APP=app
   export INDEX_FILE=/path/to/explore-index.json.gz
   flask run
   ```

## Audio Storage

Episode audio is read either from a local `audio/` directory or from an S3
bucket. The default (`auto`) picks S3 when a bucket is configured and the local
directory otherwise, so existing local setups keep working untouched.

```bash
# Local audio directory (the historical behaviour)
python run.py --data-dir /home/data/explore

# S3, no local audio needed
python run.py --audio-source s3 --s3-bucket explore-datastore
```

AWS credentials come from the standard boto3 chain: environment variables,
`~/.aws/credentials`, or an instance role. They are never read from app config.

The bucket mirrors the local layout, so `audio/<source>/<episode>.opus` on disk
becomes `s3://<bucket>/<prefix>/<source>/<episode>.opus`.

## Authentication

Sign-in is provided by the hosting platform, which serves `/xhost-auth/login`
and `/xhost-auth/logout` and returns a signed identity cookie that the app
verifies. There is no OAuth client ID or secret to configure.

For local development, set `APP_ENV=development` and `TS_USER_EMAIL` to bypass
sign-in.

## Environment Variables

- `EXPLORE_AUTH_AUDIENCES`: Comma-separated hostnames whose identity tokens this
  deployment accepts (needed for a custom domain; defaults to the request host)
- `EXPLORE_AUDIO_SOURCE`: `auto` (default), `local` or `s3`
- `EXPLORE_S3_BUCKET`: Bucket holding the audio tree
- `EXPLORE_S3_PREFIX`: Key prefix within the bucket (default `audio`)
- `EXPLORE_S3_REGION`: Bucket region (falls back to `AWS_REGION`/`AWS_DEFAULT_REGION`)
- `EXPLORE_S3_ENDPOINT_URL`: Custom endpoint for S3-compatible storage (optional)
- `EXPLORE_S3_PRESIGN_TTL`: Lifetime in seconds of presigned URLs handed to FFmpeg (default 3600)
- `INDEX_FILE`: Path to a pre-generated index file (optional)
- `FLASK_APP`: Set to "app" to run the Flask application
- `FLASK_ENV`: Set to "development" for development mode
- `SECRET_KEY`: Secret key for Flask sessions
- `POSTHOG_API_KEY`: PostHog API key for analytics (optional)
- `POSTHOG_HOST`: PostHog host URL (optional)
- `DISABLE_ANALYTICS`: Set to "true" to disable analytics

## Directory Structure

- `app/`: Main application package
  - `services/`: Core services including index management
  - `routes/`: Web application routes
  - `templates/`: HTML templates
  - `static/`: Static files
  - `cli.py`: Command line interface
- `data/`: Data directory
  - `json/`: Transcript JSON files
  - `audio/`: Audio files (optional when using the S3 backend)

## Project Structure

- `app/` - Main application code
  - `routes/` - Flask route definitions
  - `services/` - Business logic and data services
  - `static/` - CSS, JavaScript, and images
  - `templates/` - HTML templates

## Data Format

The application expects JSON transcript files with the following structure:
```json
[
  {
    "start": 0.0,
    "text": "Transcript text segment"
  },
  ...
]
```

## License

This project is licensed under the MIT License. The data accessible through this application is licensed under the [ivrit.ai license](https://www.ivrit.ai/en/the-license/).

## Contact

For support, help, ideas, and contributions, please contact us at [info@ivrit.ai](mailto:info@ivrit.ai).
