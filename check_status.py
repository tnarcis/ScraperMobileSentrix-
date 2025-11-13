#!/usr/bin/env python3

import sqlite3
import os
from datetime import datetime

def check_database_status():
    print("=== DATABASE STATUS CHECK ===")
    print(f"Time: {datetime.now()}")
    
    if not os.path.exists('mobilesentrix.db'):
        print("❌ Database file not found!")
        return
    
    try:
        conn = sqlite3.connect('mobilesentrix.db')
        cur = conn.cursor()
        
        # Check tables
        tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        print(f"\n📊 Tables: {[t[0] for t in tables]}")
        
        # Check job status
        jobs = cur.execute("SELECT COUNT(*) FROM scraping_jobs").fetchone()[0]
        print(f"\n🔄 Total Jobs: {jobs}")
        
        if jobs > 0:
            recent_job = cur.execute("""
                SELECT id, status, created_at, completed_at, total_categories, total_products 
                FROM scraping_jobs 
                ORDER BY created_at DESC 
                LIMIT 1
            """).fetchone()
            
            if recent_job:
                print(f"📍 Latest Job: ID={recent_job[0]}, Status={recent_job[1]}")
                print(f"   Created: {recent_job[2]}")
                print(f"   Completed: {recent_job[3]}")
                print(f"   Categories: {recent_job[4]}, Products: {recent_job[5]}")
        
        # Check categories
        categories = cur.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        print(f"\n📂 Total Categories: {categories}")
        
        if categories > 0:
            sample_cats = cur.execute("SELECT name, url FROM categories LIMIT 3").fetchall()
            for cat in sample_cats:
                print(f"   - {cat[0]}: {cat[1]}")
        
        # Check products
        products = cur.execute("SELECT COUNT(*) FROM scraped_products").fetchone()[0]
        print(f"\n📦 Total Products: {products}")
        
        if products > 0:
            sample_products = cur.execute("SELECT title, price FROM scraped_products LIMIT 3").fetchall()
            for prod in sample_products:
                print(f"   - {prod[0]}: {prod[1]}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Database error: {e}")

def check_recent_logs():
    print("\n=== RECENT ACTIVITY ===")
    
    # Check if there's a log file or app output
    if os.path.exists('app.log'):
        print("📝 Found app.log")
        with open('app.log', 'r') as f:
            lines = f.readlines()
            print("Last 10 lines:")
            for line in lines[-10:]:
                print(f"   {line.strip()}")
    else:
        print("📝 No app.log file found")

if __name__ == "__main__":
    check_database_status()
    check_recent_logs()