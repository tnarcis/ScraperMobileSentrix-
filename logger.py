"""
JSON Logger Module for MobileSentrix Tool v8
============================================
Structured logging with consistent JSON format for all scraping operations.
"""

import json
import datetime
import logging
import sys
from typing import Optional, Dict, Any
import pytz

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Timezone for consistent timestamps
UTC_TZ = pytz.UTC

class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record):
        """Format log record as JSON"""
        log_data = {
            'ts': datetime.datetime.now(UTC_TZ).isoformat(),
            'level': record.levelname,
            'event': getattr(record, 'event', 'general'),
            'message': record.getMessage()
        }
        
        # Add custom fields if present
        for field in ['site', 'url', 'page', 'items', 't_fetch_ms', 't_parse_ms', 't_db_ms', 'cf_detected', 'error']:
            if hasattr(record, field):
                log_data[field] = getattr(record, field)
        
        return json.dumps(log_data)

# Setup JSON handler
json_handler = logging.StreamHandler(sys.stdout)
json_handler.setFormatter(JSONFormatter())

# Create structured logger
json_logger = logging.getLogger('mobilesentrix.json')
json_logger.setLevel(logging.INFO)
json_logger.addHandler(json_handler)
json_logger.propagate = False

def log_scrape_start(site: str, url: str, **kwargs):
    """Log scraping start event"""
    # Filter out reserved keywords that conflict with LogRecord
    safe_kwargs = {k: v for k, v in kwargs.items() if k not in ['msg', 'message', 'args']}
    json_logger.info(
        f"Started scraping {site}",
        extra={'event': 'scrape_start', 'site': site, 'url': url, **safe_kwargs}
    )

def log_scrape_page(site: str, url: str, page: int, items: int, t_fetch_ms: float, 
                   t_parse_ms: float, t_db_ms: float, cf_detected: bool = False, **kwargs):
    """Log page scraping event"""
    json_logger.info(
        f"Scraped page {page} from {site}: {items} items",
        extra={
            'event': 'scrape_page',
            'site': site,
            'url': url,
            'page': page,
            'items': items,
            't_fetch_ms': t_fetch_ms,
            't_parse_ms': t_parse_ms,
            't_db_ms': t_db_ms,
            'cf_detected': cf_detected,
            **kwargs
        }
    )

def log_scrape_complete(site: str, total_pages: int, total_items: int, **kwargs):
    """Log scraping completion event"""
    json_logger.info(
        f"Completed scraping {site}: {total_pages} pages, {total_items} items",
        extra={
            'event': 'scrape_complete',
            'site': site,
            'page': total_pages,
            'items': total_items,
            **kwargs
        }
    )

def log_scrape_error(site: str, url: str, error: str, **kwargs):
    """Log scraping error event"""
    # Filter out reserved keywords that conflict with LogRecord
    safe_kwargs = {k: v for k, v in kwargs.items() if k not in ['msg', 'message', 'args']}
    json_logger.error(
        f"Scraping error on {site}: {error}",
        extra={
            'event': 'scrape_error',
            'site': site,
            'url': url,
            'error': error,
            **safe_kwargs
        }
    )

def log_discovery(site: str, categories_found: int, **kwargs):
    """Log category discovery event"""
    # Filter out reserved keywords that conflict with LogRecord
    safe_kwargs = {k: v for k, v in kwargs.items() if k not in ['msg', 'message', 'args']}
    json_logger.info(
        f"Discovery complete for {site}: {categories_found} categories",
        extra={
            'event': 'discovery',
            'site': site,
            'items': categories_found,
            **safe_kwargs
        }
    )

def log_cf_detected(site: str, url: str, **kwargs):
    """Log Cloudflare detection event"""
    json_logger.warning(
        f"Cloudflare protection detected on {site}",
        extra={
            'event': 'cf_detected',
            'site': site,
            'url': url,
            'cf_detected': True,
            **kwargs
        }
    )

def log_job_start(job_id: str, client: str, **kwargs):
    """Log background job start"""
    # Filter out reserved keywords that conflict with LogRecord
    safe_kwargs = {k: v for k, v in kwargs.items() if k not in ['msg', 'message', 'args']}
    json_logger.info(
        f"Background job started: {job_id} for {client}",
        extra={
            'event': 'job_start',
            'job_id': job_id,
            'site': client,
            **safe_kwargs
        }
    )

def log_job_complete(job_id: str, client: str, **kwargs):
    """Log background job completion"""
    json_logger.info(
        f"Background job completed: {job_id} for {client}",
        extra={
            'event': 'job_complete',
            'job_id': job_id,
            'site': client,
            **kwargs
        }
    )

def log_job_error(job_id: str, client: str, error: str, **kwargs):
    """Log background job error"""
    json_logger.error(
        f"Background job error: {job_id} for {client} - {error}",
        extra={
            'event': 'job_error',
            'job_id': job_id,
            'site': client,
            'error': error,
            **kwargs
        }
    )