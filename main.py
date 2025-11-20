import os
import asyncio
import time
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import db, create_document, get_documents

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TelegramConfig(BaseModel):
    bot_token: str
    chat_id: str


GAZETTE_URL = (
    "https://gazette.gov.mv/iulaan?type=&job-category=&office=%DE%8A%DE%AA%DE%82%DE%A6%DE%8B%DE%AB&q=&start-date=&end-date="
)


def fetch_gazette_posts() -> List[dict]:
    """Fetch and parse posts from the Gazette page. Returns list of dicts with title and url."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    }
    r = requests.get(GAZETTE_URL, headers=headers, timeout=20)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Gazette returned status {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")

    posts: List[dict] = []
    # The site structure may change; try to be resilient
    # Find all cards/rows with an anchor to the post
    for a in soup.select('a[href^="/iulaan/view/"]'):
        title = a.get_text(strip=True)
        href = a.get("href")
        if not title or not href:
            continue
        url = (
            href if href.startswith("http") else f"https://gazette.gov.mv{href}"
        )
        # Avoid duplicates within a single fetch
        if not any(p["url"] == url for p in posts):
            posts.append({"title": title, "url": url})

    # Fallback: list items
    if not posts:
        for li in soup.select("li a"):
            href = li.get("href")
            title = li.get_text(strip=True)
            if href and title and "/iulaan/view/" in href:
                url = (
                    href if href.startswith("http") else f"https://gazette.gov.mv{href}"
                )
                if not any(p["url"] == url for p in posts):
                    posts.append({"title": title, "url": url})

    return posts


def store_new_posts(posts: List[dict]) -> List[dict]:
    """Store posts if not in DB yet. Returns the list of newly inserted posts."""
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")

    existing = get_documents("gazettepost", {})
    existing_urls = {doc.get("url") for doc in existing}

    new_posts = []
    for p in posts:
        if p["url"] not in existing_urls:
            create_document("gazettepost", {"title": p["title"], "url": p["url"], "notified": False})
            new_posts.append(p)
    return new_posts


def send_telegram_message(bot_token: str, chat_id: str, text: str) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    try:
        res = requests.post(url, json=payload, timeout=20)
        return res.status_code == 200
    except Exception:
        return False


@app.get("/")
async def root():
    return {"message": "Gazette watcher backend running"}


@app.get("/api/fetch")
async def api_fetch():
    posts = fetch_gazette_posts()
    new_posts = store_new_posts(posts)
    return {"fetched": len(posts), "new": len(new_posts), "new_posts": new_posts}


@app.post("/api/notify")
async def api_notify(cfg: TelegramConfig):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")

    # Get unnotified posts (most recent first by created_at)
    docs = list(db["gazettepost"].find({"notified": False}).sort("created_at", -1).limit(20))
    if not docs:
        return {"sent": 0}

    sent = 0
    for d in reversed(docs):  # Send oldest first for order
        title = d.get("title", "New Gazette Post")
        url = d.get("url", "")
        ok = send_telegram_message(cfg.bot_token, cfg.chat_id, f"{title}\n{url}")
        if ok:
            db["gazettepost"].update_one({"_id": d["_id"]}, {"$set": {"notified": True, "updated_at": time.time()}})
            sent += 1
        else:
            # stop on failure to avoid rate-limits or invalid config
            break

    return {"sent": sent}


@app.get("/api/posts")
async def api_posts(limit: Optional[int] = 50):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    docs = list(db["gazettepost"].find().sort("created_at", -1).limit(int(limit)))
    # Convert ObjectId to string
    for d in docs:
        d["_id"] = str(d["_id"])
        if "created_at" in d:
            d["created_at"] = str(d["created_at"])
        if "updated_at" in d:
            d["updated_at"] = str(d["updated_at"])
    return {"items": docs}


# Background scheduler without external deps
_scheduler_task: asyncio.Task | None = None


async def _scheduler_loop():
    # wait a little before first fetch
    await asyncio.sleep(5)
    while True:
        try:
            posts = fetch_gazette_posts()
            store_new_posts(posts)
        except Exception:
            pass
        # 30 minutes
        await asyncio.sleep(1800)


@app.on_event("startup")
async def on_startup():
    global _scheduler_task
    if _scheduler_task is None:
        _scheduler_task = asyncio.create_task(_scheduler_loop())


@app.on_event("shutdown")
async def on_shutdown():
    global _scheduler_task
    if _scheduler_task is not None:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except Exception:
            pass
        _scheduler_task = None


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
