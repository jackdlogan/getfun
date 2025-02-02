# GetFun Token Scanner

Real-time token scanner for monitoring PumpPortal. Automatically stores token information in Supabase and maintains a local CSV backup.

## Features
- Real-time WebSocket monitoring
- Automatic token detection
- Supabase database integration
- CSV backup storage
- Automatic reconnection
- Railway.app deployment ready

## Setup
1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables in `.env`:
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

3. Run the scanner:
```bash
python pump_token_scanner.py
```

## Deployment
Configured for Railway.app deployment. Required environment variables:
- `SUPABASE_URL`
- `SUPABASE_KEY`